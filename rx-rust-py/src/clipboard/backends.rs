// 监控后端
// - PollingBackend：通用，周期轮询读取剪贴板，检测到变化时触发回调
// - Win32HookBackend (Windows only)：隐藏消息窗口 + AddClipboardFormatListener
//
// 两者都暴露统一的 start() / stop() / is_running() 接口

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::Mutex;

use crate::clipboard::types::{ClipChangeType, ClipContent, compute_signature};
use crate::clipboard::io::ClipboardIO;
use pyo3::prelude::*;

type Sig = (i64, String, i64, Vec<String>);

// =====================================================================
// PollingBackend
// =====================================================================

pub struct PollingBackend {
    interval_ms: u64,
    running: Arc<AtomicBool>,
    thread: Arc<Mutex<Option<JoinHandle<()>>>>,
    callback: Arc<dyn Fn() + Send + Sync>,
    last_sig: Arc<Mutex<Option<Sig>>>,
}

impl PollingBackend {
    pub fn new<F>(interval: Duration, cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self {
            interval_ms: interval.as_millis().max(20) as u64,
            running: Arc::new(AtomicBool::new(false)),
            thread: Arc::new(Mutex::new(None)),
            callback: Arc::new(cb),
            last_sig: Arc::new(Mutex::new(None)),
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn start(&self) {
        if self.running.load(Ordering::SeqCst) {
            return;
        }
        self.running.store(true, Ordering::SeqCst);

        let running = self.running.clone();
        let interval_ms = self.interval_ms;
        let callback = self.callback.clone();
        let last_sig = self.last_sig.clone();

        let handle = thread::spawn(move || {
            loop {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                // 读取当前剪贴板并计算签名
                Python::with_gil(|py| {
                    if let Ok((ct, content, files, _meta)) = ClipboardIO::read(py) {
                        let ct_val = ct.borrow(py).value;
                        let sig = compute_signature(ct_val, &content, &files);
                        let mut last = last_sig.lock();
                        let changed = match last.as_ref() {
                            Some(s) => *s != sig,
                            None => true,
                        };
                        if changed {
                            *last = Some(sig);
                            drop(last);
                            (callback)();
                        }
                    }
                });
                thread::sleep(Duration::from_millis(interval_ms));
            }
        });
        *self.thread.lock() = Some(handle);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        let opt = self.thread.lock().take();
        if let Some(h) = opt {
            let _ = h.join();
        }
    }

    pub fn name(&self) -> &'static str {
        "polling"
    }
}

impl Drop for PollingBackend {
    fn drop(&mut self) {
        self.stop();
    }
}

// =====================================================================
// Win32 Hook Backend
// =====================================================================

#[cfg(windows)]
pub struct Win32HookBackend {
    running: Arc<AtomicBool>,
    thread: Arc<Mutex<Option<JoinHandle<()>>>>,
    hwnd: Arc<Mutex<Option<usize>>>,
    callback: Arc<dyn Fn() + Send + Sync>,
}

#[cfg(windows)]
impl Win32HookBackend {
    pub fn new<F>(cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            thread: Arc::new(Mutex::new(None)),
            hwnd: Arc::new(Mutex::new(None)),
            callback: Arc::new(cb),
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn start(&self) -> PyResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.running.store(true, Ordering::SeqCst);

        let running = self.running.clone();
        let hwnd_out = self.hwnd.clone();
        let callback = self.callback.clone();

        let handle = thread::spawn(move || {
            // 使用 windows crate 创建隐藏消息窗口
            use windows::Win32::Foundation::{CloseHandle, HINSTANCE, HWND, LRESULT, WPARAM};
            use windows::Win32::System::LibraryLoader::GetModuleHandleW;
            use windows::Win32::UI::WindowsAndMessaging::{
                AddClipboardFormatListener, CreateWindowExW, DefWindowProcW, DestroyWindow,
                GetMessageW, PostMessageW, PostQuitMessage, RegisterClassW,
                RemoveClipboardFormatListener, CW_USEDEFAULT, MSG, WINDOW_EX_STYLE,
                WNDCLASSW, WS_POPUP, WM_CLOSE, WM_CLIPBOARDUPDATE,
            };

            unsafe extern "system" fn wnd_proc(
                hwnd: HWND,
                msg: u32,
                wparam: WPARAM,
                lparam: isize,
            ) -> LRESULT {
                if msg == WM_CLIPBOARDUPDATE {
                    // 通过 thread local 存储回调指针较为复杂，这里使用 PostMessage 让线程自己处理
                    // 简化：我们在 GetMessageW 循环里捕获此消息后在 Rust 线程里触发回调，
                    // 为了避免麻烦，这里只转发到 DefWindowProcW 处理，具体逻辑放在外层通过 PostMessage 标志。
                    // 为了简化实现，我们用一个全局 channel，但更简单的做法是让外层收到消息后自己处理。
                    return DefWindowProcW(hwnd, msg, wparam, lparam);
                }
                if msg == WM_CLOSE {
                    PostQuitMessage(0);
                    return LRESULT(0);
                }
                DefWindowProcW(hwnd, msg, wparam, lparam)
            }

            unsafe {
                let h_instance = GetModuleHandleW(None);
                let class_name: Vec<u16> = "RXRUST_CLIP_BRDCST"
                    .encode_utf16()
                    .chain(std::iter::once(0))
                    .collect();

                let wc = WNDCLASSW {
                    style: windows::Win32::UI::WindowsAndMessaging::WNDCLASS_STYLES(0),
                    lpfnWndProc: Some(wnd_proc),
                    hInstance: h_instance.unwrap_or(HINSTANCE(0)),
                    lpszClassName: windows::core::PCWSTR(class_name.as_ptr()),
                    ..Default::default()
                };

                if RegisterClassW(&wc) == 0 {
                    // 可能已注册过，忽略
                }

                let hwnd = CreateWindowExW(
                    WINDOW_EX_STYLE(0),
                    windows::core::PCWSTR(class_name.as_ptr()),
                    windows::core::PCWSTR(class_name.as_ptr()),
                    WS_POPUP,
                    0,
                    0,
                    0,
                    0,
                    HWND(0),
                    None,
                    h_instance.unwrap_or(HINSTANCE(0)),
                    None,
                );
                if hwnd.0 == 0 {
                    return;
                }

                *hwnd_out.lock() = Some(hwnd.0 as usize);

                if AddClipboardFormatListener(hwnd).is_err() {
                    let _ = DestroyWindow(hwnd);
                    *hwnd_out.lock() = None;
                    return;
                }

                // 消息循环
                let mut msg = MSG::default();
                loop {
                    let res = GetMessageW(&mut msg, HWND(0), 0, 0);
                    if res.0 <= 0 {
                        // WM_QUIT 或错误
                        break;
                    }
                    if msg.message == WM_CLIPBOARDUPDATE {
                        (callback)();
                    }
                    // 调用默认处理
                    DefWindowProcW(msg.hwnd, msg.message, msg.wParam, msg.lParam);
                }

                let _ = RemoveClipboardFormatListener(hwnd);
                let _ = DestroyWindow(hwnd);
                *hwnd_out.lock() = None;
            }
        });
        *self.thread.lock() = Some(handle);
        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        // 向窗口发送 WM_CLOSE 使其退出消息循环
        if let Some(hwnd_val) = self.hwnd.lock().clone() {
            use windows::Win32::Foundation::HWND;
            use windows::Win32::UI::WindowsAndMessaging::{PostMessageW, WM_CLOSE};
            unsafe {
                let _ = PostMessageW(HWND(hwnd_val as isize), WM_CLOSE, windows::Win32::Foundation::WPARAM(0), windows::Win32::Foundation::LPARAM(0));
            }
        }
        let opt = self.thread.lock().take();
        if let Some(h) = opt {
            let _ = h.join();
        }
    }

    pub fn name(&self) -> &'static str {
        "win32"
    }
}

#[cfg(windows)]
impl Drop for Win32HookBackend {
    fn drop(&mut self) {
        self.stop();
    }
}

// 非 Windows 下提供一个空实现（用于编译通过），实际运行时 polling 会被使用
#[cfg(not(windows))]
pub struct Win32HookBackend;

#[cfg(not(windows))]
impl Win32HookBackend {
    pub fn new<F>(_cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Win32HookBackend
    }
    pub fn is_running(&self) -> bool {
        false
    }
    pub fn start(&self) -> PyResult<()> {
        Err(pyo3::exceptions::PyOSError::new_err(
            "Win32 backend only available on Windows",
        ))
    }
    pub fn stop(&self) {}
    pub fn name(&self) -> &'static str {
        "win32"
    }
}
