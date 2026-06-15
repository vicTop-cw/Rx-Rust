// Keyboard Mouse 相关的 Rust 实现模块。

pub mod backends; // Win32 Hook + Polling 后端
pub mod dispatcher; // KeyboardDispatcher / MouseDispatcher
pub mod io; // 键鼠 I/O 封装 (跨平台)
pub mod observer; // KeyObserver / MouseObserver 观察者
pub mod subject; // KeySubject / MouseSubject 主题封装
pub mod toplevel;
pub mod types; // KeyData / MouseData / KeyEventType / MouseEventType 类型 // 顶层工厂函数和写入操作符

pub use dispatcher::{KeyboardDispatcher, MouseDispatcher};
pub use io::{KeyboardIO, MouseIO};
pub use observer::{KeyObserver, MouseObserver};
pub use subject::{KeySubject, MouseSubject};
pub use toplevel::*;
pub use types::{KeyData, KeyEventType, KeyModifier, MouseData, MouseEventType};
