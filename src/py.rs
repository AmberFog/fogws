use std::sync::Arc;

use pyo3::{
    IntoPyObjectExt, create_exception,
    exceptions::PyException,
    prelude::*,
    types::{PyAny, PyBytes, PyModule, PyString},
};
use tokio_tungstenite::tungstenite::Message;

use crate::{
    client,
    config::{
        ConnectionConfig, DEFAULT_CLOSE_TIMEOUT_SECONDS, DEFAULT_MAX_BUFFERED_BYTES,
        DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_QUEUE,
    },
    driver::{ConnectionDriver, InboundMessage},
    error::DriverError,
    runtime,
};

create_exception!(fogws, FogWSError, PyException);
create_exception!(fogws, ConnectionClosed, FogWSError);
create_exception!(fogws, ConnectionClosedError, ConnectionClosed);
create_exception!(fogws, ConnectionClosedOK, ConnectionClosed);
create_exception!(fogws, ConcurrencyError, FogWSError);
create_exception!(fogws, ConnectionFailedError, FogWSError);
create_exception!(fogws, InvalidURIError, FogWSError);
create_exception!(fogws, LoopAffinityError, FogWSError);
create_exception!(fogws, ResourceLimitError, FogWSError);
create_exception!(fogws, RuntimeContextError, FogWSError);

#[pyclass(name = "_Connection", module = "fogws._fogws", frozen)]
pub struct PyConnection {
    driver: Arc<ConnectionDriver>,
    owner_loop: Py<PyAny>,
}

#[pymethods]
impl PyConnection {
    fn _abort(&self) {
        self.driver.abort();
    }

    fn send_text<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyString>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_loop_affinity(py)?;
        let driver = Arc::clone(&self.driver);
        let payload = payload.to_str()?;
        let admission = driver
            .try_admit_outbound(payload.len())
            .map_err(driver_error_to_python)?;
        let payload = payload.to_owned();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            driver
                .send_admitted(admission, Message::Text(payload.into()))
                .await
                .map_err(driver_error_to_python)
        })
    }

    fn send_bytes<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_loop_affinity(py)?;
        let driver = Arc::clone(&self.driver);
        let payload = payload.as_bytes();
        let admission = driver
            .try_admit_outbound(payload.len())
            .map_err(driver_error_to_python)?;
        let payload = payload.to_vec();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            driver
                .send_admitted(admission, Message::Binary(payload.into()))
                .await
                .map_err(driver_error_to_python)
        })
    }

    fn receive<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_loop_affinity(py)?;
        let driver = Arc::clone(&self.driver);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let message = driver.receive().await.map_err(driver_error_to_python)?;
            Python::attach(|py| match message {
                InboundMessage::Text(payload) => payload.into_py_any(py),
                InboundMessage::Binary(payload) => {
                    Ok(PyBytes::new(py, &payload).into_any().unbind())
                }
            })
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_loop_affinity(py)?;
        let driver = Arc::clone(&self.driver);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            driver.close().await.map_err(driver_error_to_python)
        })
    }
}

impl PyConnection {
    fn ensure_loop_affinity(&self, py: Python<'_>) -> PyResult<()> {
        runtime::ensure_current_context(py)?;
        let current_loop = current_loop(py)?;
        if !current_loop.is(&self.owner_loop) {
            return Err(LoopAffinityError::new_err(
                "connection operations must run on the asyncio loop that created the connection",
            ));
        }
        Ok(())
    }
}

#[pyfunction(name = "_connect")]
#[pyo3(signature = (
    uri,
    *,
    max_queue=None,
    max_message_size=None,
    max_buffered_bytes=None,
    close_timeout=10.0,
))]
fn connect(
    py: Python<'_>,
    uri: String,
    max_queue: Option<Py<PyAny>>,
    max_message_size: Option<Py<PyAny>>,
    max_buffered_bytes: Option<Py<PyAny>>,
    close_timeout: f64,
) -> PyResult<Bound<'_, PyAny>> {
    runtime::ensure_current_context(py)?;
    let owner_loop = current_loop(py)?.unbind();
    let config = ConnectionConfig::new(
        extract_limit(py, max_queue, "max_queue", DEFAULT_MAX_QUEUE)?,
        extract_limit(
            py,
            max_message_size,
            "max_message_size",
            DEFAULT_MAX_MESSAGE_SIZE,
        )?,
        extract_limit(
            py,
            max_buffered_bytes,
            "max_buffered_bytes",
            DEFAULT_MAX_BUFFERED_BYTES,
        )?,
        close_timeout,
    )
    .map_err(driver_error_to_python)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let driver = client::connect(uri, config)
            .await
            .map_err(driver_error_to_python)?;
        Ok(PyConnection { driver, owner_loop })
    })
}

fn extract_limit(
    py: Python<'_>,
    value: Option<Py<PyAny>>,
    name: &str,
    default: usize,
) -> PyResult<usize> {
    let Some(value) = value else {
        return Ok(default);
    };
    value.bind(py).extract::<usize>().map_err(|_| {
        ResourceLimitError::new_err(format!(
            "{name} must be a non-negative platform-sized integer",
        ))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add_class::<PyConnection>()?;
    module.add_function(wrap_pyfunction!(connect, module)?)?;
    module.add("FogWSError", py.get_type::<FogWSError>())?;
    module.add("ConnectionClosed", py.get_type::<ConnectionClosed>())?;
    module.add(
        "ConnectionClosedError",
        py.get_type::<ConnectionClosedError>(),
    )?;
    module.add("ConnectionClosedOK", py.get_type::<ConnectionClosedOK>())?;
    module.add("ConcurrencyError", py.get_type::<ConcurrencyError>())?;
    module.add(
        "ConnectionFailedError",
        py.get_type::<ConnectionFailedError>(),
    )?;
    module.add("InvalidURIError", py.get_type::<InvalidURIError>())?;
    module.add("LoopAffinityError", py.get_type::<LoopAffinityError>())?;
    module.add("ResourceLimitError", py.get_type::<ResourceLimitError>())?;
    module.add("RuntimeContextError", py.get_type::<RuntimeContextError>())?;
    module.add("DEFAULT_CLOSE_TIMEOUT", DEFAULT_CLOSE_TIMEOUT_SECONDS)?;
    module.add("DEFAULT_MAX_BUFFERED_BYTES", DEFAULT_MAX_BUFFERED_BYTES)?;
    module.add("DEFAULT_MAX_MESSAGE_SIZE", DEFAULT_MAX_MESSAGE_SIZE)?;
    module.add("DEFAULT_MAX_QUEUE", DEFAULT_MAX_QUEUE)?;
    Ok(())
}

fn current_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::get_current_loop(py).map_err(|_| {
        LoopAffinityError::new_err(
            "FogWS async operations must be called from a running asyncio event loop",
        )
    })
}

fn driver_error_to_python(error: DriverError) -> PyErr {
    match error {
        DriverError::ClosedError(info) => {
            let error = ConnectionClosedError::new_err(info.describe());
            attach_close_info(error, info.code, info.reason, Some(info.initiated_by_local))
        }
        DriverError::ClosedOk(info) => {
            let error = ConnectionClosedOK::new_err(info.describe());
            attach_close_info(error, info.code, info.reason, Some(info.initiated_by_local))
        }
        DriverError::Concurrency(message) => ConcurrencyError::new_err(message),
        DriverError::ConnectionFailed(message) => ConnectionFailedError::new_err(message),
        DriverError::InvalidUri(message) => InvalidURIError::new_err(message),
        DriverError::ResourceLimit(message) => ResourceLimitError::new_err(message),
        DriverError::Transport(message) => {
            let error = ConnectionClosedError::new_err(message);
            attach_close_info(error, None, String::new(), None)
        }
    }
}

fn attach_close_info(
    error: PyErr,
    code: Option<u16>,
    reason: String,
    initiated_by_local: Option<bool>,
) -> PyErr {
    let result = Python::attach(|py| {
        let value = error.value(py);
        value.setattr("code", code)?;
        value.setattr("reason", reason)?;
        value.setattr("initiated_by_local", initiated_by_local)
    });
    match result {
        Ok(()) => error,
        Err(attribute_error) => attribute_error,
    }
}
