// 键鼠底层 I/O 封装
// Windows：使用 windows crate 的 Win32 API 模拟输入
// 其他平台：打印提示信息（不支持）

use pyo3::prelude::*;

// =====================================================================
// Windows 实现（基于 windows crate）
// =====================================================================

#[cfg(windows)]
mod win_impl {
    use super::*;
    use windows::Win32::System::SystemServices::GetAsyncKeyState;
    use windows::Win32::UI::Input::KeyboardAndMouse::POINT;
    use windows::Win32::UI::Input::KeyboardAndMouse::{
        GetCursorPos, INPUT_MOUSE, MOUSEEVENTF_ABSOLUTE, MOUSEEVENTF_LEFTDOWN, MOUSEEVENTF_LEFTUP,
        MOUSEEVENTF_MIDDLEDOWN, MOUSEEVENTF_MIDDLEUP, MOUSEEVENTF_MOVE, MOUSEEVENTF_RIGHTDOWN,
        MOUSEEVENTF_RIGHTUP, MOUSEEVENTF_WHEEL, MOUSEINPUT,
    };
    use windows::Win32::UI::Input::KeyboardAndMouse::{
        SendInput, VkKeyScanW, INPUT, INPUT_0, INPUT_KEYBOARD, KEYBDINPUT, KEYEVENTF_KEYUP,
        KEYEVENTF_UNICODE,
    };
    use windows::Win32::UI::WindowsAndMessaging::GetSystemMetrics;
    use windows::Win32::UI::WindowsAndMessaging::SM_CXSCREEN;
    use windows::Win32::UI::WindowsAndMessaging::SM_CYSCREEN;
    use windows::Win32::UI::WindowsAndMessaging::{GetForegroundWindow, GetWindowTextW};

    // 辅助函数：发送单个键盘输入事件
    fn send_keyboard_input(key_code: u16, flags: u32) -> PyResult<()> {
        let input = INPUT {
            r#type: INPUT_KEYBOARD,
            Anonymous: INPUT_0 {
                ki: KEYBDINPUT {
                    wVk: key_code,
                    wScan: 0,
                    dwFlags: flags,
                    time: 0,
                    dwExtraInfo: 0,
                },
            },
        };
        let inputs = [input];
        let result = unsafe { SendInput(&inputs, std::mem::size_of::<INPUT>() as i32) };
        if result == 0 {
            println!("[keyboard_mouse] SendInput 失败，key_code={}", key_code);
        }
        Ok(())
    }

    // 辅助函数：发送单个鼠标输入事件
    fn send_mouse_input(dx: i32, dy: i32, mouse_data: u32, flags: u32) -> PyResult<()> {
        let input = INPUT {
            r#type: INPUT_MOUSE,
            Anonymous: INPUT_0 {
                mi: MOUSEINPUT {
                    dx,
                    dy,
                    mouseData: mouse_data,
                    dwFlags: flags,
                    time: 0,
                    dwExtraInfo: 0,
                },
            },
        };
        let inputs = [input];
        let result = unsafe { SendInput(&inputs, std::mem::size_of::<INPUT>() as i32) };
        if result == 0 {
            println!("[keyboard_mouse] SendInput (mouse) 失败");
        }
        Ok(())
    }

    // 键盘 I/O 实现
    pub struct KeyboardIO;

    impl KeyboardIO {
        // 按下指定虚拟键码
        pub fn press_key(key_code: u32) -> PyResult<()> {
            println!("[keyboard_mouse] press_key: key_code={}", key_code);
            send_keyboard_input(key_code as u16, 0)
        }

        // 释放指定键
        pub fn release_key(key_code: u32) -> PyResult<()> {
            println!("[keyboard_mouse] release_key: key_code={}", key_code);
            send_keyboard_input(key_code as u16, KEYEVENTF_KEYUP)
        }

        // 按下并立即释放
        pub fn send_key(key_code: u32) -> PyResult<()> {
            println!("[keyboard_mouse] send_key: key_code={}", key_code);
            send_keyboard_input(key_code as u16, 0)?;
            send_keyboard_input(key_code as u16, KEYEVENTF_KEYUP)
        }

        // 发送一串文本
        pub fn type_text(text: &str) -> PyResult<()> {
            println!("[keyboard_mouse] type_text: {}", text);
            for ch in text.chars() {
                if ch.is_ascii() {
                    // ASCII 字符：使用 VkKeyScanW
                    let vk_result = unsafe { VkKeyScanW(ch as u16) };
                    let vk = (vk_result & 0xff) as u16;
                    let shift = (vk_result >> 8) & 1;
                    if shift != 0 {
                        send_keyboard_input(0x10, 0)?; // VK_SHIFT
                    }
                    send_keyboard_input(vk, 0)?;
                    send_keyboard_input(vk, KEYEVENTF_KEYUP)?;
                    if shift != 0 {
                        send_keyboard_input(0x10, KEYEVENTF_KEYUP)?;
                    }
                } else {
                    // Unicode 字符：使用 KEYEVENTF_UNICODE
                    send_keyboard_input(ch as u16, KEYEVENTF_UNICODE)?;
                    send_keyboard_input(ch as u16, KEYEVENTF_UNICODE | KEYEVENTF_KEYUP)?;
                }
            }
            Ok(())
        }

        // 同时按下多个键再同时释放（用于 Ctrl+C 等）
        pub fn hotkey(keys: &[u32]) -> PyResult<()> {
            println!("[keyboard_mouse] hotkey: {:?}", keys);
            // 按下所有键
            for &key in keys {
                send_keyboard_input(key as u16, 0)?;
            }
            // 释放所有键（逆序）
            for &key in keys.iter().rev() {
                send_keyboard_input(key as u16, KEYEVENTF_KEYUP)?;
            }
            Ok(())
        }

        // 获取当前焦点窗口标题
        pub fn get_foreground_window_title() -> Option<String> {
            let hwnd = unsafe { GetForegroundWindow() };
            if hwnd.0.is_null() {
                return None;
            }
            let mut buffer: [u16; 512] = [0; 512];
            let len = unsafe { GetWindowTextW(hwnd, &mut buffer) };
            if len == 0 {
                return None;
            }
            Some(String::from_utf16_lossy(&buffer[..len as usize]))
        }

        // 查询按键是否按下（用于 polling 后端）
        pub fn get_async_key_state(key_code: i32) -> bool {
            let state = unsafe { GetAsyncKeyState(key_code) };
            (state & 0x8000) != 0
        }
    }

    // 鼠标 I/O 实现
    pub struct MouseIO;

    impl MouseIO {
        // 移动鼠标到绝对坐标
        pub fn move_to(x: i32, y: i32) -> PyResult<()> {
            println!("[keyboard_mouse] move_to: x={}, y={}", x, y);
            let cx = unsafe { GetSystemMetrics(SM_CXSCREEN) };
            let cy = unsafe { GetSystemMetrics(SM_CYSCREEN) };
            // 将坐标转换为 0-65535 范围
            let dx = (x as f64 / cx as f64 * 65535.0) as i32;
            let dy = (y as f64 / cy as f64 * 65535.0) as i32;
            send_mouse_input(dx, dy, 0, MOUSEEVENTF_MOVE | MOUSEEVENTF_ABSOLUTE)
        }

        // 相对移动
        pub fn move_relative(dx: i32, dy: i32) -> PyResult<()> {
            println!("[keyboard_mouse] move_relative: dx={}, dy={}", dx, dy);
            send_mouse_input(dx, dy, 0, MOUSEEVENTF_MOVE)
        }

        // 点击鼠标按钮
        pub fn click(button: &str) -> PyResult<()> {
            println!("[keyboard_mouse] click: button={}", button);
            match button {
                "left" => {
                    send_mouse_input(0, 0, 0, MOUSEEVENTF_LEFTDOWN)?;
                    send_mouse_input(0, 0, 0, MOUSEEVENTF_LEFTUP)?;
                }
                "right" => {
                    send_mouse_input(0, 0, 0, MOUSEEVENTF_RIGHTDOWN)?;
                    send_mouse_input(0, 0, 0, MOUSEEVENTF_RIGHTUP)?;
                }
                "middle" => {
                    send_mouse_input(0, 0, 0, MOUSEEVENTF_MIDDLEDOWN)?;
                    send_mouse_input(0, 0, 0, MOUSEEVENTF_MIDDLEUP)?;
                }
                _ => {
                    println!("[keyboard_mouse] click: 不支持的按钮 {}, 使用 left", button);
                    send_mouse_input(0, 0, 0, MOUSEEVENTF_LEFTDOWN)?;
                    send_mouse_input(0, 0, 0, MOUSEEVENTF_LEFTUP)?;
                }
            }
            Ok(())
        }

        // 垂直滚动（正数向上，负数向下）
        pub fn scroll(delta: i32) -> PyResult<()> {
            println!("[keyboard_mouse] scroll: delta={}", delta);
            const WHEEL_DELTA: u32 = 120;
            send_mouse_input(0, 0, delta as u32 * WHEEL_DELTA as i32, MOUSEEVENTF_WHEEL)
        }

        // 获取当前光标位置
        pub fn get_cursor_pos() -> PyResult<(i32, i32)> {
            let mut point = POINT { x: 0, y: 0 };
            let ok = unsafe { GetCursorPos(&mut point) };
            if ok.is_ok() {
                println!(
                    "[keyboard_mouse] get_cursor_pos: ({}, {})",
                    point.x, point.y
                );
                Ok((point.x, point.y))
            } else {
                println!("[keyboard_mouse] get_cursor_pos 失败");
                Ok((0, 0))
            }
        }
    }
}

// =====================================================================
// 非 Windows 实现（空桩）
// =====================================================================

#[cfg(not(windows))]
mod unix_impl {
    use super::*;

    pub struct KeyboardIO;

    impl KeyboardIO {
        pub fn press_key(key_code: u32) -> PyResult<()> {
            println!(
                "[keyboard_mouse] press_key: 仅在 Windows 下支持，跳过 key_code={}",
                key_code
            );
            Ok(())
        }

        pub fn release_key(key_code: u32) -> PyResult<()> {
            println!(
                "[keyboard_mouse] release_key: 仅在 Windows 下支持，跳过 key_code={}",
                key_code
            );
            Ok(())
        }

        pub fn send_key(key_code: u32) -> PyResult<()> {
            println!(
                "[keyboard_mouse] send_key: 仅在 Windows 下支持，跳过 key_code={}",
                key_code
            );
            Ok(())
        }

        pub fn type_text(text: &str) -> PyResult<()> {
            println!(
                "[keyboard_mouse] type_text: 仅在 Windows 下支持，跳过 text={}",
                text
            );
            Ok(())
        }

        pub fn hotkey(keys: &[u32]) -> PyResult<()> {
            println!("[keyboard_mouse] hotkey: 仅在 Windows 下支持，跳过");
            Ok(())
        }

        pub fn get_foreground_window_title() -> Option<String> {
            println!("[keyboard_mouse] get_foreground_window_title: 仅在 Windows 下支持");
            None
        }

        pub fn get_async_key_state(key_code: i32) -> bool {
            println!(
                "[keyboard_mouse] get_async_key_state: 仅在 Windows 下支持，跳过 key_code={}",
                key_code
            );
            false
        }
    }

    pub struct MouseIO;

    impl MouseIO {
        pub fn move_to(x: i32, y: i32) -> PyResult<()> {
            println!(
                "[keyboard_mouse] move_to: 仅在 Windows 下支持，跳过 x={}, y={}",
                x, y
            );
            Ok(())
        }

        pub fn move_relative(dx: i32, dy: i32) -> PyResult<()> {
            println!(
                "[keyboard_mouse] move_relative: 仅在 Windows 下支持，跳过 dx={}, dy={}",
                dx, dy
            );
            Ok(())
        }

        pub fn click(button: &str) -> PyResult<()> {
            println!(
                "[keyboard_mouse] click: 仅在 Windows 下支持，跳过 button={}",
                button
            );
            Ok(())
        }

        pub fn scroll(delta: i32) -> PyResult<()> {
            println!(
                "[keyboard_mouse] scroll: 仅在 Windows 下支持，跳过 delta={}",
                delta
            );
            Ok(())
        }

        pub fn get_cursor_pos() -> PyResult<(i32, i32)> {
            println!("[keyboard_mouse] get_cursor_pos: 仅在 Windows 下支持");
            Ok((0, 0))
        }
    }
}

// =====================================================================
// 统一的公共 API
// =====================================================================

pub struct KeyboardIO;

impl KeyboardIO {
    pub fn press_key(key_code: u32) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::KeyboardIO::press_key(key_code)
        }
        #[cfg(not(windows))]
        {
            unix_impl::KeyboardIO::press_key(key_code)
        }
    }

    pub fn release_key(key_code: u32) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::KeyboardIO::release_key(key_code)
        }
        #[cfg(not(windows))]
        {
            unix_impl::KeyboardIO::release_key(key_code)
        }
    }

    pub fn send_key(key_code: u32) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::KeyboardIO::send_key(key_code)
        }
        #[cfg(not(windows))]
        {
            unix_impl::KeyboardIO::send_key(key_code)
        }
    }

    pub fn type_text(text: &str) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::KeyboardIO::type_text(text)
        }
        #[cfg(not(windows))]
        {
            unix_impl::KeyboardIO::type_text(text)
        }
    }

    pub fn hotkey(keys: &[u32]) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::KeyboardIO::hotkey(keys)
        }
        #[cfg(not(windows))]
        {
            unix_impl::KeyboardIO::hotkey(keys)
        }
    }

    pub fn get_foreground_window_title() -> Option<String> {
        #[cfg(windows)]
        {
            win_impl::KeyboardIO::get_foreground_window_title()
        }
        #[cfg(not(windows))]
        {
            unix_impl::KeyboardIO::get_foreground_window_title()
        }
    }

    pub fn get_async_key_state(key_code: i32) -> bool {
        #[cfg(windows)]
        {
            win_impl::KeyboardIO::get_async_key_state(key_code)
        }
        #[cfg(not(windows))]
        {
            unix_impl::KeyboardIO::get_async_key_state(key_code)
        }
    }
}

pub struct MouseIO;

impl MouseIO {
    pub fn move_to(x: i32, y: i32) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::MouseIO::move_to(x, y)
        }
        #[cfg(not(windows))]
        {
            unix_impl::MouseIO::move_to(x, y)
        }
    }

    pub fn move_relative(dx: i32, dy: i32) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::MouseIO::move_relative(dx, dy)
        }
        #[cfg(not(windows))]
        {
            unix_impl::MouseIO::move_relative(dx, dy)
        }
    }

    pub fn click(button: &str) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::MouseIO::click(button)
        }
        #[cfg(not(windows))]
        {
            unix_impl::MouseIO::click(button)
        }
    }

    pub fn scroll(delta: i32) -> PyResult<()> {
        #[cfg(windows)]
        {
            win_impl::MouseIO::scroll(delta)
        }
        #[cfg(not(windows))]
        {
            unix_impl::MouseIO::scroll(delta)
        }
    }

    pub fn get_cursor_pos() -> PyResult<(i32, i32)> {
        #[cfg(windows)]
        {
            win_impl::MouseIO::get_cursor_pos()
        }
        #[cfg(not(windows))]
        {
            unix_impl::MouseIO::get_cursor_pos()
        }
    }
}
