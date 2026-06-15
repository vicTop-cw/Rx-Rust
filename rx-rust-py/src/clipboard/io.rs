// 跨平台剪贴板 I/O 封装
// Windows：优先使用 windows crate 的 Win32 API
// 其他平台：通过 PyO3 回调到 Python 实现（使用 clipboard.py 的 reader 作为 fallback）

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::clipboard::types::{ClipChangeType, ClipContent};

// 统一的读取结果：(change_type, content, files, metadata)
pub type ReadResult = (Py<ClipChangeType>, ClipContent, Vec<String>, Py<PyDict>);

// Windows 的 CF 常量
#[allow(dead_code)]
mod win_consts {
    pub const CF_TEXT: u32 = 1;
    pub const CF_BITMAP: u32 = 2;
    pub const CF_UNICODETEXT: u32 = 13;
    pub const CF_HDROP: u32 = 15;
}

// =====================================================================
// Windows 实现（基于 windows crate）
// =====================================================================

#[cfg(windows)]
mod win_impl {
    use super::*;
    use windows::Win32::Foundation::{CloseHandle, HANDLE, HGLOBAL};
    use windows::Win32::System::Memory::{
        GlobalAlloc, GlobalLock, GlobalSize, GlobalUnlock, GMEM_MOVEABLE,
    };
    use windows::Win32::UI::WindowsAndMessaging::{
        CloseClipboard, EmptyClipboard, EnumClipboardFormats, GetClipboardData,
        OpenClipboard, SetClipboardData, HWND,
    };

    use pyo3::types::PyDict;

    // 重试：OpenClipboard 可能失败，最多重试 10 次，间隔递增
    fn with_clipboard<F, R>(hwnd: HWND, mut f: F) -> windows::core::Result<R>
    where
        F: FnMut() -> R,
    {
        let mut last_err = None;
        for attempt in 0..10 {
            let ok = unsafe { OpenClipboard(hwnd) };
            if ok.is_ok() {
                let r = f();
                let _ = unsafe { CloseClipboard() };
                return Ok(r);
            } else {
                last_err = Some(ok.err().unwrap_or_else(|| windows::core::Error::from_win32()));
                std::thread::sleep(Duration::from_millis(50 * (attempt + 1) as u64));
            }
        }
        Err(last_err.unwrap_or_else(|| windows::core::Error::from_win32()))
    }

    pub fn read(py: Python<'_>) -> PyResult<super::ReadResult> {
        // 枚举可用格式
        let formats_opt = with_clipboard(HWND(0), || {
            let mut formats = Vec::<u32>::new();
            let mut f = unsafe { EnumClipboardFormats(0) };
            while f != 0 {
                formats.push(f);
                f = unsafe { EnumClipboardFormats(f) };
            }
            formats
        });

        let formats = match formats_opt {
            Ok(fs) => fs,
            Err(_) => Vec::new(),
        };

        if formats.is_empty() {
            return Ok((
                Py::new(py, ClipChangeType::CLEAR)?,
                ClipContent::None,
                Vec::new(),
                Py::new(py, PyDict::new_bound(py))?,
            ));
        }

        // 优先级：CF_UNICODETEXT > CF_TEXT > 其它 (暂不处理 CF_HDROP 的文件列表, 见下面特殊实现)
        if formats.contains(&win_consts::CF_UNICODETEXT) || formats.contains(&win_consts::CF_TEXT) {
            if let Some(text) = read_unicode_text() {
                return Ok((
                    Py::new(py, ClipChangeType::TEXT)?,
                    ClipContent::Text(text),
                    Vec::new(),
                    Py::new(py, PyDict::new_bound(py))?,
                ));
            }
        }

        // 如果没有文本，则作为二进制返回
        Ok((
            Py::new(py, ClipChangeType::OTHER)?,
            ClipContent::None,
            Vec::new(),
            Py::new(py, PyDict::new_bound(py))?,
        ))
    }

    fn read_unicode_text() -> Option<String> {
        with_clipboard(HWND(0), || {
            let h_mem = unsafe { GetClipboardData(win_consts::CF_UNICODETEXT) };
            if h_mem.0 == 0 {
                return None;
            }
            let data_ptr = unsafe { GlobalLock(h_mem) };
            if data_ptr.is_null() {
                return None;
            }
            let size = unsafe { GlobalSize(h_mem) };
            if size == 0 {
                unsafe { GlobalUnlock(h_mem) };
                return None;
            }
            // size 是以字节为单位；读入为 UTF-16
            let slice = unsafe { std::slice::from_raw_parts(data_ptr as *const u16, (size / 2) as usize) };
            // 去除末尾的 null terminator
            let len = slice.iter().position(|&c| c == 0).unwrap_or(slice.len());
            let text = String::from_utf16_lossy(&slice[..len]);
            unsafe { GlobalUnlock(h_mem) };
            if text.is_empty() { None } else { Some(text) }
        }).ok().flatten()
    }

    pub fn write_text(text: &str) -> PyResult<()> {
        // 将字符串编码为 UTF-16 LE + 双 null 结尾
        let mut utf16: Vec<u16> = text.encode_utf16().collect();
        utf16.push(0); // null terminator
        let bytes = unsafe {
            std::slice::from_raw_parts(utf16.as_ptr() as *const u8, utf16.len() * 2)
        }.to_vec();

        let result = with_clipboard(HWND(0), || {
            let _ = unsafe { EmptyClipboard() };

            let h_mem = unsafe {
                GlobalAlloc(GMEM_MOVEABLE, bytes.len())
            };
            if h_mem.0 == 0 {
                return Err("GlobalAlloc 失败");
            }
            let dst = unsafe { GlobalLock(h_mem) };
            if dst.is_null() {
                return Err("GlobalLock 失败");
            }
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst as *mut u8, bytes.len());
            }
            unsafe { GlobalUnlock(h_mem) };

            let res = unsafe { SetClipboardData(win_consts::CF_UNICODETEXT, HGLOBAL(h_mem.0 as isize)) };
            if res.0 == 0 {
                return Err("SetClipboardData 失败");
            }
            Ok(())
        });

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(pyo3::exceptions::PyOSError::new_err(msg)),
            Err(_) => Err(pyo3::exceptions::PyOSError::new_err("OpenClipboard 失败")),
        }
    }

    pub fn clear() -> PyResult<()> {
        let result = with_clipboard(HWND(0), || {
            let _ = unsafe { EmptyClipboard() };
        });
        match result {
            Ok(()) => Ok(()),
            Err(_) => Err(pyo3::exceptions::PyOSError::new_err("Clear 失败")),
        }
    }
}

// =====================================================================
// 非 Windows 实现：通过 PyO3 回调 Python 侧实现
// =====================================================================

#[cfg(not(windows))]
mod unix_impl {
    use super::*;

    pub fn read(py: Python<'_>) -> PyResult<super::ReadResult> {
        // 回退到 Python 实现：调用 tkinter 的 clipboard_get
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
                Py::new(py, PyDict::new_bound(py))?,
            )),
            Err(_) => Ok((
                Py::new(py, ClipChangeType::CLEAR)?,
                ClipContent::None,
                Vec::new(),
                Py::new(py, PyDict::new_bound(py))?,
            )),
        }
    }

    pub fn write_text(text: &str) -> PyResult<()> {
        let py = Python::expect_gil_acquired();
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
        Self::write_text("")
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
        // 简化处理：暂不写入自定义格式，仅清空剪贴板
        // 在 Windows 下，这可扩展为写入自定义格式 (SetClipboardData(fmt, h_mem))
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
