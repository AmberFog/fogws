mod client;
mod config;
mod driver;
mod error;
mod py;
mod runtime;

#[cfg(test)]
mod driver_tests;

use pyo3::prelude::*;

/// Register the private native module used by the Python package.
#[pymodule]
fn _fogws(module: &Bound<'_, PyModule>) -> PyResult<()> {
    runtime::validate_import_context(module.py())?;
    py::register(module)?;
    runtime::configure();
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
