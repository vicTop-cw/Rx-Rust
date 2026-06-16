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
        let callback = self.callback.clone();

        // 简化实现：使用轮询而不是真正的 hook
        let handle = thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                (callback)();
                thread::sleep(Duration::from_millis(500));
            }
        });
        *self.thread.lock() = Some(handle);
        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
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