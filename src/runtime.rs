use std::sync::OnceLock;

use pyo3::prelude::*;

use crate::py::RuntimeContextError;

pub const RUNTIME_WORKER_THREADS: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeOwner {
    interpreter: usize,
    process: u32,
}

static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();

pub fn configure(py: Python<'_>) -> PyResult<()> {
    claim_or_validate_context(py)?;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .thread_name("fogws-runtime")
        .worker_threads(RUNTIME_WORKER_THREADS);
    pyo3_async_runtimes::tokio::init(builder);
    Ok(())
}

pub fn ensure_current_context(py: Python<'_>) -> PyResult<()> {
    claim_or_validate_context(py)
}

fn claim_or_validate_context(py: Python<'_>) -> PyResult<()> {
    let current_process = std::process::id();
    let current_interpreter = interpreter_identity(py)?;
    let current = RuntimeOwner {
        interpreter: current_interpreter,
        process: current_process,
    };
    let owner = OWNER.get_or_init(|| current);

    if owner.process != current.process {
        return Err(RuntimeContextError::new_err(
            "FogWS was initialized before fork; create a fresh interpreter process before using it",
        ));
    }
    if owner.interpreter != current.interpreter {
        return Err(RuntimeContextError::new_err(
            "FogWS supports only the Python interpreter that initialized its process-wide runtime",
        ));
    }
    Ok(())
}

fn interpreter_identity(py: Python<'_>) -> PyResult<usize> {
    Ok(py.import("sys")?.getattr("modules")?.as_ptr() as usize)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier, OnceLock},
        thread,
    };

    use super::RuntimeOwner;

    #[test]
    fn pyo3_runtime_is_a_process_singleton() {
        let first = std::ptr::from_ref(pyo3_async_runtimes::tokio::get_runtime());
        let second = std::ptr::from_ref(pyo3_async_runtimes::tokio::get_runtime());

        assert_eq!(first, second);
    }

    #[test]
    fn runtime_owner_is_claimed_as_one_immutable_tuple() {
        let owner = Arc::new(OnceLock::new());
        let barrier = Arc::new(Barrier::new(2));
        let claims = [11, 22].map(|interpreter| {
            let owner = Arc::clone(&owner);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let candidate = RuntimeOwner {
                    interpreter,
                    process: 7,
                };
                barrier.wait();
                *owner.get_or_init(|| candidate) == candidate
            })
        });
        let accepted = claims
            .into_iter()
            .map(|claim| claim.join().unwrap())
            .filter(|accepted| *accepted)
            .count();

        assert_eq!(accepted, 1);
    }
}
