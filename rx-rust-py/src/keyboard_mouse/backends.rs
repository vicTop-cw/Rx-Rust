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

/// 原始键盘事件
#[derive(Debug, Clone)]
pub enum RawKeyEvent {
    KeyDown(u32), // 虚拟键码
    KeyUp(u32),
}

/// 原始鼠标事件
#[derive(Debug, Clone)]
pub enum RawMouseEvent {
    Move(i32, i32), // x, y
    LeftDown(i32, i32),
    LeftUp(i32, i32),
    RightDown(i32, i32),
    RightUp(i32, i32),
    MiddleDown(i32, i32),
    MiddleUp(i32, i32),
    Scroll(i32, i32, i32), // x, y, delta
    Wheel(i32, i32, i32),  // x, y, delta (同 Scroll)
}

// =====================================================================
// Backend Trait 定义
// =====================================================================

/// 键盘后端 trait
pub trait KeyboardBackend: Send + Sync {
    fn start(&self, tx: mpsc::Sender<RawKeyEvent>) -> Result<(), String>;
    fn stop(&self);
    fn is_running(&self) -> bool;
}

/// 鼠标后端 trait
pub trait MouseBackend: Send + Sync {
    fn start(&self, tx: mpsc::Sender<RawMouseEvent>) -> Result<(), String>;
    fn stop(&self);
    fn is_running(&self) -> bool;
}

// =====================================================================
// PollingBackend - 跨平台轮询后端
// =====================================================================

/// 轮询式键盘后端（跨平台）
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
        // 上次按键状态：key_code -> 是否按下
        let last_state: Arc<std::sync::Mutex<std::collections::HashMap<u32, bool>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

        let last_state_clone = last_state.clone();

        let handle = thread::spawn(move || {
            // 遍历 0..256 虚拟键码检测状态变化
            while running.load(Ordering::SeqCst) {
                for key_code in 0u32..256 {
                    let is_pressed =
                        crate::keyboard_mouse::io::KeyboardIO::get_async_key_state(key_code as i32);
                    let mut last = last_state_clone.lock().unwrap();
                    let prev = *last.get(&key_code).unwrap_or(&false);

                    if is_pressed && !prev {
                        // 按下
                        let _ = tx.send(RawKeyEvent::KeyDown(key_code));
                    } else if !is_pressed && prev {
                        // 释放
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

/// 轮询式鼠标后端（跨平台）
pub struct PollingMouseBackend {
    interval_ms: u64,
    running: Arc<AtomicBool>,
    thread: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    last_pos: Arc<std::sync::Mutex<(i32, i32)>>,
    // 上次鼠标按钮状态
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
                // 获取当前鼠标位置
                let (x, y) = crate::keyboard_mouse::io::MouseIO::get_cursor_pos().unwrap_or((0, 0));

                // 检测位置变化
                {
                    let mut last = last_pos.lock().unwrap();
                    if last.0 != x || last.1 != y {
                        let _ = tx.send(RawMouseEvent::Move(x, y));
                        *last = (x, y);
                    }
                }

                // 检测左键
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

                // 检测右键
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

                // 检测中键
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
// =====================================================================

#[cfg(windows)]
pub mod win32 {
    use super::*;
    use std::sync::mpsc::{channel, Sender};
    use windows::Win32::Foundation::{LPARAM, LRESULT, WPARAM};
    use windows::Win32::UI::Input::KeyboardAndMouse::{
        KBDLLHOOKSTRUCT, MSLLHOOKSTRUCT, WH_KEYBOARD_LL, WH_MOUSE_LL,
    };
    use windows::Win32::UI::WindowsAndMessaging::{
        CallNextHookEx, DispatchMessageW, PeekMessageW, SetWindowsHookExW, TranslateMessage,
        UnhookWindowsHookEx, MSG, PM_REMOVE, WM_KEYDOWN, WM_KEYUP, WM_LBUTTONDOWN, WM_LBUTTONUP,
        WM_MBUTTONDOWN, WM_MBUTTONUP, WM_MOUSEMOVE, WM_MOUSEWHEEL, WM_QUIT, WM_RBUTTONDOWN,
        WM_RBUTTONUP, WM_SYSKEYDOWN, WM_SYSKEYUP,
    };

    // 全局键盘 Sender，用于在钩子回调和消息循环线程之间共享
    // 使用 Mutex 包装以便在钩子回调中安全访问
    struct GlobalKeySender {
        tx: std::sync::Mutex<Option<Sender<RawKeyEvent>>>,
    }

    static mut GLOBAL_KEY_SENDER: *const GlobalKeySender = std::ptr::null_mut();

    // 全局鼠标 Sender
    struct GlobalMouseSender {
        tx: std::sync::Mutex<Option<Sender<RawMouseEvent>>>,
    }

    static mut GLOBAL_MOUSE_SENDER: *const GlobalMouseSender = std::ptr::null_mut();

    /// Win32 低级键盘钩子后端
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

        fn start_hook(&self, tx: Sender<RawKeyEvent>) -> Result<(), String> {
            let running = self.running.clone();

            // 初始化全局 Sender
            unsafe {
                let gs = Box::new(GlobalKeySender {
                    tx: std::sync::Mutex::new(Some(tx)),
                });
                GLOBAL_KEY_SENDER = Box::into_raw(gs);
            }

            let handle = thread::spawn(move || {
                unsafe extern "system" fn keyboard_hook_proc(
                    code: i32,
                    wparam: WPARAM,
                    lparam: LPARAM,
                ) -> LRESULT {
                    if code >= 0 {
                        let tx_opt = {
                            let guard = (*GLOBAL_KEY_SENDER).tx.lock().unwrap();
                            guard.clone()
                        };
                        if let Some(tx) = tx_opt {
                            let vk = {
                                let kb_struct = *(lparam.0 as *const KBDLLHOOKSTRUCT);
                                kb_struct.vkCode
                            };

                            let event = match wparam.0 as u32 {
                                WM_KEYDOWN | WM_SYSKEYDOWN => Some(RawKeyEvent::KeyDown(vk)),
                                WM_KEYUP | WM_SYSKEYUP => Some(RawKeyEvent::KeyUp(vk)),
                                _ => None,
                            };

                            if let Some(e) = event {
                                let _ = tx.send(e);
                            }
                        }
                    }
                    unsafe {
                        CallNextHookEx(
                            windows::Win32::Foundation::HHOOK::default(),
                            code,
                            wparam,
                            lparam,
                        )
                    }
                }

                // 设置键盘钩子
                let hook =
                    unsafe { SetWindowsHookExW(WH_KEYBOARD_LL, keyboard_hook_proc, None, 0) };

                match hook {
                    Ok(h) => {
                        // 运行消息循环使钩子生效
                        let mut msg = MSG::default();
                        while !running.load(Ordering::Relaxed) {
                            if unsafe { PeekMessageW(&mut msg, None, 0, 0, PM_REMOVE) }.as_bool() {
                                if msg.message == WM_QUIT {
                                    break;
                                }
                                unsafe {
                                    TranslateMessage(&msg);
                                    DispatchMessageW(&msg);
                                }
                            }
                            thread::sleep(Duration::from_millis(5));
                        }

                        // 清理钩子
                        unsafe {
                            let _ = UnhookWindowsHookEx(h);
                        };
                    }
                    Err(e) => {
                        eprintln!(
                            "[keyboard_mouse] SetWindowsHookExW (keyboard) failed: {:?}",
                            e
                        );
                    }
                }

                // 清理全局 Sender
                unsafe {
                    if !GLOBAL_KEY_SENDER.is_null() {
                        let _ = Box::from_raw(GLOBAL_KEY_SENDER);
                        GLOBAL_KEY_SENDER = std::ptr::null_mut();
                    }
                }
            });

            *self.thread.lock().unwrap() = Some(handle);
            Ok(())
        }
    }

    impl KeyboardBackend for Win32KeyboardBackend {
        fn start(&self, tx: Sender<RawKeyEvent>) -> Result<(), String> {
            if self.running.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.running.store(true, Ordering::SeqCst);
            self.start_hook(tx)
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

    /// Win32 低级鼠标钩子后端
    pub struct Win32MouseBackend {
        running: Arc<AtomicBool>,
        thread: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    }

    impl Win32MouseBackend {
        pub fn new() -> Self {
            Self {
                running: Arc::new(AtomicBool::new(false)),
                thread: Arc::new(std::sync::Mutex::new(None)),
            }
        }

        fn start_hook(&self, tx: Sender<RawMouseEvent>) -> Result<(), String> {
            let running = self.running.clone();

            // 初始化全局 Sender
            unsafe {
                let gs = Box::new(GlobalMouseSender {
                    tx: std::sync::Mutex::new(Some(tx)),
                });
                GLOBAL_MOUSE_SENDER = Box::into_raw(gs);
            }

            let handle = thread::spawn(move || {
                unsafe extern "system" fn mouse_hook_proc(
                    code: i32,
                    wparam: WPARAM,
                    lparam: LPARAM,
                ) -> LRESULT {
                    if code >= 0 {
                        let tx_opt = {
                            let guard = (*GLOBAL_MOUSE_SENDER).tx.lock().unwrap();
                            guard.clone()
                        };
                        if let Some(tx) = tx_opt {
                            let event = match wparam.0 as u32 {
                                WM_LBUTTONDOWN => {
                                    let pt = {
                                        let ms_struct = *(lparam.0 as *const MSLLHOOKSTRUCT);
                                        ms_struct.pt
                                    };
                                    Some(RawMouseEvent::LeftDown(pt.x, pt.y))
                                }
                                WM_LBUTTONUP => {
                                    let pt = {
                                        let ms_struct = *(lparam.0 as *const MSLLHOOKSTRUCT);
                                        ms_struct.pt
                                    };
                                    Some(RawMouseEvent::LeftUp(pt.x, pt.y))
                                }
                                WM_RBUTTONDOWN => {
                                    let pt = {
                                        let ms_struct = *(lparam.0 as *const MSLLHOOKSTRUCT);
                                        ms_struct.pt
                                    };
                                    Some(RawMouseEvent::RightDown(pt.x, pt.y))
                                }
                                WM_RBUTTONUP => {
                                    let pt = {
                                        let ms_struct = *(lparam.0 as *const MSLLHOOKSTRUCT);
                                        ms_struct.pt
                                    };
                                    Some(RawMouseEvent::RightUp(pt.x, pt.y))
                                }
                                WM_MBUTTONDOWN => {
                                    let pt = {
                                        let ms_struct = *(lparam.0 as *const MSLLHOOKSTRUCT);
                                        ms_struct.pt
                                    };
                                    Some(RawMouseEvent::MiddleDown(pt.x, pt.y))
                                }
                                WM_MBUTTONUP => {
                                    let pt = {
                                        let ms_struct = *(lparam.0 as *const MSLLHOOKSTRUCT);
                                        ms_struct.pt
                                    };
                                    Some(RawMouseEvent::MiddleUp(pt.x, pt.y))
                                }
                                WM_MOUSEMOVE => {
                                    let pt = {
                                        let ms_struct = *(lparam.0 as *const MSLLHOOKSTRUCT);
                                        ms_struct.pt
                                    };
                                    Some(RawMouseEvent::Move(pt.x, pt.y))
                                }
                                WM_MOUSEWHEEL => {
                                    let ms_struct = *(lparam.0 as *const MSLLHOOKSTRUCT);
                                    let delta = (ms_struct.mouseData as i32) >> 16;
                                    Some(RawMouseEvent::Scroll(
                                        ms_struct.pt.x,
                                        ms_struct.pt.y,
                                        delta,
                                    ))
                                }
                                _ => None,
                            };

                            if let Some(e) = event {
                                let _ = tx.send(e);
                            }
                        }
                    }
                    unsafe {
                        CallNextHookEx(
                            windows::Win32::Foundation::HHOOK::default(),
                            code,
                            wparam,
                            lparam,
                        )
                    }
                }

                // 设置鼠标钩子
                let hook = unsafe { SetWindowsHookExW(WH_MOUSE_LL, mouse_hook_proc, None, 0) };

                match hook {
                    Ok(h) => {
                        // 运行消息循环使钩子生效
                        let mut msg = MSG::default();
                        while !running.load(Ordering::Relaxed) {
                            if unsafe { PeekMessageW(&mut msg, None, 0, 0, PM_REMOVE) }.as_bool() {
                                if msg.message == WM_QUIT {
                                    break;
                                }
                                unsafe {
                                    TranslateMessage(&msg);
                                    DispatchMessageW(&msg);
                                }
                            }
                            thread::sleep(Duration::from_millis(5));
                        }

                        // 清理钩子
                        unsafe {
                            let _ = UnhookWindowsHookEx(h);
                        };
                    }
                    Err(e) => {
                        eprintln!("[keyboard_mouse] SetWindowsHookExW (mouse) failed: {:?}", e);
                    }
                }

                // 清理全局 Sender
                unsafe {
                    if !GLOBAL_MOUSE_SENDER.is_null() {
                        let _ = Box::from_raw(GLOBAL_MOUSE_SENDER);
                        GLOBAL_MOUSE_SENDER = std::ptr::null_mut();
                    }
                }
            });

            *self.thread.lock().unwrap() = Some(handle);
            Ok(())
        }
    }

    impl MouseBackend for Win32MouseBackend {
        fn start(&self, tx: Sender<RawMouseEvent>) -> Result<(), String> {
            if self.running.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.running.store(true, Ordering::SeqCst);
            self.start_hook(tx)
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

// 非 Windows 平台：Win32 后端直接返回错误
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

pub use self::keyboard::{KeyboardBackend, PollingKeyboardBackend};
pub use self::mouse::{MouseBackend, PollingMouseBackend};
pub use self::raw::{RawKeyEvent, RawMouseEvent};
pub use self::win32::{Win32KeyboardBackend, Win32MouseBackend};

// 内部子模块
mod raw {
    pub use super::RawKeyEvent;
    pub use super::RawMouseEvent;
}

mod keyboard {
    pub use super::KeyboardBackend;
    pub use super::PollingKeyboardBackend;
}

mod mouse {
    pub use super::MouseBackend;
    pub use super::PollingMouseBackend;
}
