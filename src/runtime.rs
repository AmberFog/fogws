use std::sync::OnceLock;

use pyo3::{exceptions::PyImportError, prelude::*, types::PyTuple};

use crate::py::RuntimeContextError;

pub const RUNTIME_WORKER_THREADS: usize = 2;

static OWNER_PROCESS: OnceLock<u32> = OnceLock::new();

pub fn validate_import_context(py: Python<'_>) -> PyResult<()> {
    ensure_main_interpreter(py)?;
    claim_or_validate_process()
}

pub fn configure() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .thread_name("fogws-runtime")
        .worker_threads(RUNTIME_WORKER_THREADS);
    pyo3_async_runtimes::tokio::init(builder);
}

pub fn ensure_current_process() -> PyResult<()> {
    claim_or_validate_process()
}

pub fn is_owner_process() -> bool {
    OWNER_PROCESS
        .get()
        .is_none_or(|owner_process| *owner_process == std::process::id())
}

fn claim_or_validate_process() -> PyResult<()> {
    let current_process = std::process::id();
    let owner_process = OWNER_PROCESS.get_or_init(|| current_process);
    if *owner_process != current_process {
        return Err(RuntimeContextError::new_err(
            "FogWS was initialized before fork; create a fresh interpreter process before using it",
        ));
    }
    Ok(())
}

fn ensure_main_interpreter(py: Python<'_>) -> PyResult<()> {
    let interpreters = py
        .import("_interpreters")
        .or_else(|_| py.import("_xxsubinterpreters"))?;
    let current = extract_interpreter_id(&interpreters.call_method0("get_current")?)?;
    let main = extract_interpreter_id(&interpreters.call_method0("get_main")?)?;
    if current != main {
        return Err(PyImportError::new_err(
            "module fogws._fogws does not support loading in subinterpreters",
        ));
    }
    Ok(())
}

fn extract_interpreter_id(identity: &Bound<'_, PyAny>) -> PyResult<i64> {
    if let Ok(details) = identity.cast::<PyTuple>() {
        return details.get_item(0)?.extract();
    }
    identity.call_method0("__index__")?.extract()
}

#[cfg(test)]
mod tests {
    #[test]
    fn pyo3_runtime_is_a_process_singleton() {
        let first = std::ptr::from_ref(pyo3_async_runtimes::tokio::get_runtime());
        let second = std::ptr::from_ref(pyo3_async_runtimes::tokio::get_runtime());

        assert_eq!(first, second);
    }
}
