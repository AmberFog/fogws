use pyo3::prelude::*;

/// Register the private native module used by the Python package.
#[pymodule]
fn _fogws(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
