// 键鼠事件监控后端模块
// - PollingBackend：跨平台，周期轮询检测键鼠状态变化
// - Win32HookBackend (Windows only)：使用低级键盘/鼠标钩子捕获系统事件
//
// 两者都暴露统一的 start() / stop() / is_running() 接口

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

// =====================================================================
// 原始事件类型（从系统捕获的未经处理的原始事件）
// =====================================================================

#[derive(Debug, Clone)]
pub enum RawKeyEvent {
    KeyDown(u32),
    KeyUp(u32),
}

#[derive(Debug, Clone)]
pub enum RawMouseEvent {
    Move(i32, i32),
    LeftDown(i32, i32),
    LeftUp(i32, i32),
    RightDown(i32, i32),
    RightUp(i32, i32),
    MiddleDown(i32, i32),
    MiddleUp(i32, i32),
    Scroll(i32, i32, i32),
    Wheel(i32, i32, i32),
}

// =====================================================================
// Backend Trait 定义
// =====================================================================

pub trait KeyboardBackend: Send + Sync {
    fn start(&self, tx: mpsc::Sender<RawKeyEvent>) -> Result<(), String>;
    fn stop(&self);
    fn is_running(&self) -> bool;
}

pub trait MouseBackend: Send + Sync {
    fn start(&self, tx: mpsc::Sender<RawMouseEvent>) -> Result<(), String>;
    fn stop(&self);
    fn is_running(&self) -> bool;
}

// =====================================================================
// PollingBackend - 跨平台轮询后端
// =====================================================================

pub struct PollingKeyboardBackend {
    interval_ms: u64,
    running: Arc<AtomicBool>,
    thread: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl PollingKeyboardBackend {
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms: interval_ms.max(10),
            running: Arc::new(AtomicBool::new(false)),
            thread: Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

impl KeyboardBackend for PollingKeyboardBackend {
    fn start(&self, tx: mpsc::Sender<RawKeyEvent>) -> Result<(), String> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.running.store(true, Ordering::SeqCst);

        let running = self.running.clone();
        let interval_ms = self.interval_ms;
        let last_state: Arc<std::sync::Mutex<std::collections::HashMap<u32, bool>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

        let last_state_clone = last_state.clone();

        let handle = thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                for key_code in 0u32..256 {
                    let is_pressed =
                        crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(key_code as i32);
                    let mut last = last_state_clone.lock().unwrap();
                    let prev = *last.get(&key_code).unwrap_or(&false);

                    if is_pressed && !prev {
                        let _ = tx.send(RawKeyEvent::KeyDown(key_code));
                    } else if !is_pressed && prev {
                        let _ = tx.send(RawKeyEvent::KeyUp(key_code));
                    }

                    if is_pressed != prev {
                        *last.entry(key_code).or_insert(is_pressed) = is_pressed;
                    }
                }
                thread::sleep(Duration::from_millis(interval_ms));
            }
        });

        *self.thread.lock().unwrap() = Some(handle);
        Ok(())
    }

    fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        let opt = self.thread.lock().unwrap().take();
        if let Some(h) = opt {
            let _ = h.join();
        }
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

impl Drop for PollingKeyboardBackend {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct PollingMouseBackend {
    interval_ms: u64,
    running: Arc<AtomicBool>,
    thread: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    last_pos: Arc<std::sync::Mutex<(i32, i32)>>,
    last_lbutton: Arc<std::sync::Mutex<bool>>,
    last_rbutton: Arc<std::sync::Mutex<bool>>,
    last_mbutton: Arc<std::sync::Mutex<bool>>,
}

impl PollingMouseBackend {
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms: interval_ms.max(10),
            running: Arc::new(AtomicBool::new(false)),
            thread: Arc::new(std::sync::Mutex::new(None)),
            last_pos: Arc::new(std::sync::Mutex::new((0, 0))),
            last_lbutton: Arc::new(std::sync::Mutex::new(false)),
            last_rbutton: Arc::new(std::sync::Mutex::new(false)),
            last_mbutton: Arc::new(std::sync::Mutex::new(false)),
        }
    }
}

impl MouseBackend for PollingMouseBackend {
    fn start(&self, tx: mpsc::Sender<RawMouseEvent>) -> Result<(), String> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.running.store(true, Ordering::SeqCst);

        let running = self.running.clone();
        let interval_ms = self.interval_ms;
        let last_pos = self.last_pos.clone();
        let last_lbtn = self.last_lbutton.clone();
        let last_rbtn = self.last_rbutton.clone();
        let last_mbtn = self.last_mbutton.clone();

        let handle = thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                let (x, y) = crate::keyboard_mouse::io::MouseIO::get_cursor_pos().unwrap_or((0, 0));

                {
                    let mut last = last_pos.lock().unwrap();
                    if last.0 != x || last.1 != y {
                        let _ = tx.send(RawMouseEvent::Move(x, y));
                        *last = (x, y);
                    }
                }

                let lbtn = crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(0x01);
                {
                    let mut last = last_lbtn.lock().unwrap();
                    if lbtn && !*last {
                        let _ = tx.send(RawMouseEvent::LeftDown(x, y));
                    } else if !lbtn && *last {
                        let _ = tx.send(RawMouseEvent::LeftUp(x, y));
                    }
                    *last = lbtn;
                }

                let rbtn = crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(0x02);
                {
                    let mut last = last_rbtn.lock().unwrap();
                    if rbtn && !*last {
                        let _ = tx.send(RawMouseEvent::RightDown(x, y));
                    } else if !rbtn && *last {
                        let _ = tx.send(RawMouseEvent::RightUp(x, y));
                    }
                    *last = rbtn;
                }

                let mbtn = crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(0x04);
                {
                    let mut last = last_mbtn.lock().unwrap();
                    if mbtn && !*last {
                        let _ = tx.send(RawMouseEvent::MiddleDown(x, y));
                    } else if !mbtn && *last {
                        let _ = tx.send(RawMouseEvent::MiddleUp(x, y));
                    }
                    *last = mbtn;
                }

                thread::sleep(Duration::from_millis(interval_ms));
            }
        });

        *self.thread.lock().unwrap() = Some(handle);
        Ok(())
    }

    fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        let opt = self.thread.lock().unwrap().take();
        if let Some(h) = opt {
            let _ = h.join();
        }
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

impl Drop for PollingMouseBackend {
    fn drop(&mut self) {
        self.stop();
    }
}

// =====================================================================
// Win32 Hook Backend (Windows only)
// Note: Simplified implementation - uses polling instead of hooks
// due to windows-sys API changes in version 0.52
// =====================================================================

#[cfg(windows)]
pub mod win32 {
    use super::*;
    use std::sync::mpsc::Sender;

    pub struct Win32KeyboardBackend {
        running: Arc<AtomicBool>,
        thread: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    }

    impl Win32KeyboardBackend {
        pub fn new() -> Self {
            Self {
                running: Arc::new(AtomicBool::new(false)),
                thread: Arc::new(std::sync::Mutex::new(None)),
            }
        }
    }

    impl KeyboardBackend for Win32KeyboardBackend {
        fn start(&self, tx: Sender<RawKeyEvent>) -> Result<(), String> {
            if self.running.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.running.store(true, Ordering::SeqCst);

            let running = self.running.clone();
            let last_state: Arc<std::sync::Mutex<std::collections::HashMap<u32, bool>>> =
                Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
            let last_state_clone = last_state.clone();

            let handle = thread::spawn(move || {
                while running.load(Ordering::SeqCst) {
                    for key_code in 0u32..256 {
                        let is_pressed =
                            crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(key_code as i32);
                        let mut last = last_state_clone.lock().unwrap();
                        let prev = *last.get(&key_code).unwrap_or(&false);

                        if is_pressed && !prev {
                            let _ = tx.send(RawKeyEvent::KeyDown(key_code));
                        } else if !is_pressed && prev {
                            let _ = tx.send(RawKeyEvent::KeyUp(key_code));
                        }

                        if is_pressed != prev {
                            *last.entry(key_code).or_insert(is_pressed) = is_pressed;
                        }
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            });

            *self.thread.lock().unwrap() = Some(handle);
            Ok(())
        }

        fn stop(&self) {
            self.running.store(false, Ordering::SeqCst);
            let opt = self.thread.lock().unwrap().take();
            if let Some(h) = opt {
                let _ = h.join();
            }
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    impl Drop for Win32KeyboardBackend {
        fn drop(&mut self) {
            self.stop();
        }
    }

    pub struct Win32MouseBackend {
        running: Arc<AtomicBool>,
        thread: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
        last_pos: Arc<std::sync::Mutex<(i32, i32)>>,
        last_lbutton: Arc<std::sync::Mutex<bool>>,
        last_rbutton: Arc<std::sync::Mutex<bool>>,
        last_mbutton: Arc<std::sync::Mutex<bool>>,
    }

    impl Win32MouseBackend {
        pub fn new() -> Self {
            Self {
                running: Arc::new(AtomicBool::new(false)),
                thread: Arc::new(std::sync::Mutex::new(None)),
                last_pos: Arc::new(std::sync::Mutex::new((0, 0))),
                last_lbutton: Arc::new(std::sync::Mutex::new(false)),
                last_rbutton: Arc::new(std::sync::Mutex::new(false)),
                last_mbutton: Arc::new(std::sync::Mutex::new(false)),
            }
        }
    }

    impl MouseBackend for Win32MouseBackend {
        fn start(&self, tx: Sender<RawMouseEvent>) -> Result<(), String> {
            if self.running.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.running.store(true, Ordering::SeqCst);

            let running = self.running.clone();
            let last_pos = self.last_pos.clone();
            let last_lbtn = self.last_lbutton.clone();
            let last_rbtn = self.last_rbutton.clone();
            let last_mbtn = self.last_mbutton.clone();

            let handle = thread::spawn(move || {
                while running.load(Ordering::SeqCst) {
                    let (x, y) = crate::keyboard_mouse::io::MouseIO::get_cursor_pos().unwrap_or((0, 0));

                    {
                        let mut last = last_pos.lock().unwrap();
                        if last.0 != x || last.1 != y {
                            let _ = tx.send(RawMouseEvent::Move(x, y));
                            *last = (x, y);
                        }
                    }

                    let lbtn = crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(0x01);
                    {
                        let mut last = last_lbtn.lock().unwrap();
                        if lbtn && !*last {
                            let _ = tx.send(RawMouseEvent::LeftDown(x, y));
                        } else if !lbtn && *last {
                            let _ = tx.send(RawMouseEvent::LeftUp(x, y));
                        }
                        *last = lbtn;
                    }

                    let rbtn = crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(0x02);
                    {
                        let mut last = last_rbtn.lock().unwrap();
                        if rbtn && !*last {
                            let _ = tx.send(RawMouseEvent::RightDown(x, y));
                        } else if !rbtn && *last {
                            let _ = tx.send(RawMouseEvent::RightUp(x, y));
                        }
                        *last = rbtn;
                    }

                    let mbtn = crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(0x04);
                    {
                        let mut last = last_mbtn.lock().unwrap();
                        if mbtn && !*last {
                            let _ = tx.send(RawMouseEvent::MiddleDown(x, y));
                        } else if !mbtn && *last {
                            let _ = tx.send(RawMouseEvent::MiddleUp(x, y));
                        }
                        *last = mbtn;
                    }

                    thread::sleep(Duration::from_millis(50));
                }
            });

            *self.thread.lock().unwrap() = Some(handle);
            Ok(())
        }

        fn stop(&self) {
            self.running.store(false, Ordering::SeqCst);
            let opt = self.thread.lock().unwrap().take();
            if let Some(h) = opt {
                let _ = h.join();
            }
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    impl Drop for Win32MouseBackend {
        fn drop(&mut self) {
            self.stop();
        }
    }
}

#[cfg(not(windows))]
pub mod win32 {
    use super::*;
    use std::sync::mpsc::Sender;

    pub struct Win32KeyboardBackend;

    impl Win32KeyboardBackend {
        pub fn new() -> Self {
            Self
        }
    }

    impl KeyboardBackend for Win32KeyboardBackend {
        fn start(&self, _tx: Sender<RawKeyEvent>) -> Result<(), String> {
            Err("Win32 keyboard backend only available on Windows".to_string())
        }
        fn stop(&self) {}
        fn is_running(&self) -> bool {
            false
        }
    }

    pub struct Win32MouseBackend;

    impl Win32MouseBackend {
        pub fn new() -> Self {
            Self
        }
    }

    impl MouseBackend for Win32MouseBackend {
        fn start(&self, _tx: Sender<RawMouseEvent>) -> Result<(), String> {
            Err("Win32 mouse backend only available on Windows".to_string())
        }
        fn stop(&self) {}
        fn is_running(&self) -> bool {
            false
        }
    }
}

// =====================================================================
// 导出
// =====================================================================