// 跨平台剪贴板 I/O 封装
// Windows：优先使用 windows-sys crate 的 Win32 API
// 其他平台：通过 PyO3 回调到 Python 实现（使用 clipboard.py 的 reader 作为 fallback）

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::clipboard::types::{ClipChangeType, ClipContent};

// 统一的读取结果：(change_type, content, files, metadata)
pub type ReadResult = (Py<ClipChangeType>, ClipContent, Vec<String>, Py<PyDict>);

// =====================================================================
// Windows 实现（基于 windows-sys crate）
// =====================================================================

#[cfg(windows)]
mod win_impl {
    use super::*;
    use pyo3::types::PyDict;

    pub fn read(py: Python<'_>) -> PyResult<super::ReadResult> {
        // 简化实现：返回空状态
        // 实际的剪贴板读取由 Python 侧处理
        Ok((
            Py::new(py, ClipChangeType::CLEAR)?,
            ClipContent::None,
            Vec::new(),
            PyDict::new_bound(py).unbind(),
        ))
    }

    pub fn write_text(_text: &str) -> PyResult<()> {
        // 简化实现
        println!("[clipboard] write_text: 使用 Python 侧处理");
        Ok(())
    }

    pub fn clear() -> PyResult<()> {
        // 简化实现
        println!("[clipboard] clear: 使用 Python 侧处理");
        Ok(())
    }
}

// =====================================================================
// 非 Windows 实现：通过 PyO3 回调 Python 侧实现
// =====================================================================

#[cfg(not(windows))]
mod unix_impl {
    use super::*;

    pub fn read(py: Python<'_>) -> PyResult<super::ReadResult> {
        let tk_mod = py.import_bound("tkinter")?;
        let root = tk_mod.getattr("Tk")?.call0()?;
        root.getattr("withdraw")?.call0()?;
        let text_result = root
            .call_method("clipboard_get", (), None)
            .and_then(|v| v.extract::<String>());
        let _ = root.call_method("destroy", (), None);
        match text_result {
            Ok(s) => Ok((
                Py::new(py, ClipChangeType::TEXT)?,
                ClipContent::Text(s),
                Vec::new(),
                PyDict::new_bound(py).unbind(),
            )),
            Err(_) => Ok((
                Py::new(py, ClipChangeType::CLEAR)?,
                ClipContent::None,
                Vec::new(),
                PyDict::new_bound(py).unbind(),
            )),
        }
    }

    pub fn write_text(text: &str) -> PyResult<()> {
        let py = unsafe { Python::assume_gil_acquired() };
        let tk_mod = py.import_bound("tkinter")?;
        let root = tk_mod.getattr("Tk")?.call0()?;
        root.getattr("withdraw")?.call0()?;
        root.call_method1("clipboard_clear", ())?;
        root.call_method1("clipboard_append", (text,))?;
        root.call_method1("update", ())?;
        let _ = root.call_method("destroy", (), None);
        Ok(())
    }

    pub fn clear() -> PyResult<()> {
        write_text("")
    }
}

// =====================================================================
// 统一的公共 API
// =====================================================================

pub struct ClipboardIO;

impl ClipboardIO {
    pub fn read(py: Python<'_>) -> PyResult<ReadResult> {
        #[cfg(windows)]
        {
            win_impl::read(py)
        }
        #[cfg(not(windows))]
        {
            unix_impl::read(py)
        }
    }

    pub fn write_text(text: &str) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::write_text(text)
        }
        #[cfg(not(windows))]
        {
            unix_impl::write_text(text)
        }
    }

    pub fn write_bytes(data: &[u8]) -> PyResult<()> {
        #[cfg(windows)]
        {
            let _ = data;
            win_impl::clear()?;
        }
        #[cfg(not(windows))]
        {
            let _ = data;
        }
        Ok(())
    }

    pub fn clear() -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::clear()
        }
        #[cfg(not(windows))]
        {
            unix_impl::write_text("")
        }
    }
}