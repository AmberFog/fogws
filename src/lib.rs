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
    py::register(module)?;
    runtime::configure(module.py())?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
