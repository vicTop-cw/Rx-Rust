# rx_rust.keyboard_mouse 使用指南

> **异步 Rust 实现的高性能键盘鼠标监控与模拟模块**

---

## 目录

1. [概述](#1-概述)
2. [安装与编译](#2-安装与编译)
3. [快速开始](#3-快速开始)
4. [类型系统](#4-类型系统)
5. [键盘监控](#5-键盘监控)
6. [鼠标监控](#6-鼠标监控)
7. [响应式操作符](#7-响应式操作符)
8. [模拟操作](#8-模拟操作)
9. [自我过滤机制](#9-自我过滤机制)
10. [Observer 观察者](#10-observer-观察者)
11. [完整示例](#11-完整示例)
12. [API 参考](#12-api-参考)
13. [常见问题](#13-常见问题)

---

## 1. 概述

`rx_rust.keyboard_mouse` 是 `rx-rust` 项目的一部分，提供基于 Rust + PyO3 的高性能键盘鼠标监控与模拟功能。

### 核心特性

- **异步 Rust 实现**：低延迟、高性能，绕过 Python GIL
- **双后端支持**：Windows Hook (低延迟) / Polling (跨平台兼容)
- **自我过滤**：自动避免模拟输入触发自身的死循环
- **响应式 API**：基于 RxPy 风格的订阅和流处理
- **类型安全**：完整的类型提示和 Python stub 文件

### 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                      Python 层                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  from_keyboard()│ │  from_mouse() │  │ write_to_*() │    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
│         │                 │                 │             │
│         ▼                 ▼                 ▼             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              KeyboardDispatcher / MouseDispatcher    │  │
│  │                    (事件分发器)                        │  │
│  └──────────────────────────┬──────────────────────────┘  │
└─────────────────────────────┼───────────────────────────────┘
                              │
┌─────────────────────────────┼───────────────────────────────┐
│                      Rust 层 (PyO3)                         │
│         ▼                  ▼                  ▼            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  KeyData /   │  │  Backend     │  │   Subject    │    │
│  │  MouseData   │  │ (Hook/Poll)  │  │ (发布订阅)   │    │
│  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. 安装与编译

### 2.1 编译 Rust 扩展

```bash
# 进入项目目录
cd rx-rust-py

# 开发模式（编译并安装到当前 Python 环境）
maturin develop

# 或使用 pip
pip install .

# 或构建 wheel
maturin build
```

### 2.2 前置依赖

- Python 3.9+
- Rust 1.75+
- maturin 1.0+

### 2.3 Windows 特殊说明

Windows 下需要管理员权限才能安装某些全局钩子。如果 `auto` 后端检测到权限不足，会自动回退到 `polling` 模式。

---

## 3. 快速开始

### 3.1 监听键盘事件

```python
from rx_rust import from_keyboard, KeyEventType

# 创建键盘事件流
obs, kbd = from_keyboard(backend="polling")

# 订阅按键按下事件
obs.subscribe(on_next=lambda e: print(f"按下: {e.key_name}"))

# 程序保持运行...
input("按 Enter 停止...")
kbd.stop()
```

### 3.2 监听鼠标事件

```python
from rx_rust import from_mouse, MouseEventType

# 创建鼠标事件流
obs, mouse = from_mouse(backend="polling")

# 订阅鼠标移动事件
obs.subscribe(on_next=lambda e: print(f"移动到: ({e.x}, {e.y})"))

input("按 Enter 停止...")
mouse.stop()
```

### 3.3 模拟键盘输入

```python
from rx_rust import KeyboardDispatcher

kbd = KeyboardDispatcher(backend="polling")
kbd.start()

# 模拟输入
kbd.type_text("Hello, World!")
kbd.hotkey("Ctrl", "A")  # 全选
kbd.hotkey("Ctrl", "C")  # 复制

kbd.stop()
```

### 3.4 模拟鼠标操作

```python
from rx_rust import MouseDispatcher

mouse = MouseDispatcher(backend="polling")
mouse.start()

# 移动鼠标并点击
mouse.move_to(500, 300)
mouse.click("left")
mouse.double_click("left")
mouse.scroll(120)  # 向上滚动

mouse.stop()
```

---

## 4. 类型系统

### 4.1 键盘事件类型 (KeyEventType)

```python
from rx_rust import KeyEventType

# 枚举值
KeyEventType.KEY_DOWN  # 0 - 按键按下
KeyEventType.KEY_UP    # 1 - 按键释放
KeyEventType.KEY_HOLD  # 2 - 按键持续按住（仅 Polling 后端）
```

### 4.2 鼠标事件类型 (MouseEventType)

```python
from rx_rust import MouseEventType

MouseEventType.MOVE        # 0 - 鼠标移动
MouseEventType.LEFT_DOWN   # 1 - 左键按下
MouseEventType.LEFT_UP     # 2 - 左键释放
MouseEventType.RIGHT_DOWN   # 3 - 右键按下
MouseEventType.RIGHT_UP     # 4 - 右键释放
MouseEventType.MIDDLE_DOWN  # 5 - 中键按下
MouseEventType.MIDDLE_UP    # 6 - 中键释放
MouseEventType.SCROLL       # 7 - 滚轮滚动
MouseEventType.DRAG         # 8 - 鼠标拖拽
```

### 4.3 修饰键标志 (KeyModifier)

```python
from rx_rust import KeyModifier

# 位标志组合
mod = KeyModifier.SHIFT | KeyModifier.CTRL

# 检查修饰键
if mod & KeyModifier.SHIFT:
    print("Shift 键被按下")

str(mod)  # "SHIFT+CTRL"
```

### 4.4 键盘事件数据 (KeyData)

```python
from rx_rust import KeyData

# 构造
kd = KeyData(
    key_code=65,           # 虚拟键码 (VK_A)
    key_name="A",          # 键名
    is_press=True,         # True=按下, False=释放
    event_type=KeyEventType.KEY_DOWN,  # 事件类型
    modifiers=0,           # 修饰键组合
    timestamp=1700000000000,  # 毫秒时间戳
    sequence=1,            # 全局序号
    window_title="Notepad"  # 窗口标题（可选）
)

# 序列化
kd.to_dict()    # 转字典
kd.to_json()    # 转 JSON 字符串
kd.to_pickle()  # 转 Pickle 字节

# 反序列化
kd2 = KeyData.from_dict(d)
kd3 = KeyData.from_json(json_str)
```

### 4.5 鼠标事件数据 (MouseData)

```python
from rx_rust import MouseData, MouseEventType

# 构造
md = MouseData(
    x=100,                           # X 坐标
    y=200,                           # Y 坐标
    event_type=MouseEventType.MOVE,   # 事件类型
    button="none",                    # 按钮 ("left"/"right"/"middle")
    delta=0,                          # 滚轮增量（SCROLL 时使用）
    timestamp=1700000000000,          # 毫秒时间戳
    sequence=1                        # 全局序号
)

# 序列化
md.to_dict()
md.to_json()
md.to_pickle()
```

---

## 5. 键盘监控

### 5.1 使用 KeyboardDispatcher

```python
from rx_rust import KeyboardDispatcher

# 构造分发器
kbd = KeyboardDispatcher(
    backend="auto",       # "auto" | "win32" | "polling"
    interval=0.05,        # Polling 间隔（秒）
    filter_self=True,     # 启用自我过滤
    auto_start=False      # 是否自动启动
)

# 生命周期
kbd.start()          # 启动监控
kbd.stop()           # 停止监控

# 使用上下文管理器
with KeyboardDispatcher() as kbd:
    kbd.subscribe(callback)
    # ...
# 自动停止
```

### 5.2 使用 KeySubject

```python
from rx_rust import KeySubject

# 构造主题（自动启动）
with KeySubject(backend="polling") as ks:
    # 订阅
    sub = ks.subscribe(on_next=callback)
    sub.dispose()  # 取消订阅

    # 模拟操作
    ks.type_text("hello")
    ks.hotkey("Ctrl", "S")
```

### 5.3 使用 from_keyboard 工厂

```python
from rx_rust import from_keyboard, ops

# 创建事件流
obs, kbd = from_keyboard(
    backend="polling",
    interval=0.05,
    filter_self=True,
    auto_start=True,
    self_filter=None  # 自定义过滤函数
)

# 过滤特定按键
obs.pipe(
    ops.filter(lambda e: e.key_name == "A"),
    ops.filter(lambda e: e.is_press)
).subscribe(on_next=lambda e: print("按下了 A"))

kbd.stop()
```

### 5.4 后端选择

| 后端 | 平台 | 延迟 | 权限需求 |
|------|------|------|----------|
| `auto` | Windows | 低 | 管理员 |
| `win32` | Windows | 低 | 管理员 |
| `polling` | 所有 | 中等 | 普通用户 |

```python
# 自动选择（Windows 优先 Hook，回退 Polling）
kbd = KeyboardDispatcher(backend="auto")

# 强制使用 Polling
kbd = KeyboardDispatcher(backend="polling")

# 强制使用 Win32 Hook
kbd = KeyboardDispatcher(backend="win32")
```

---

## 6. 鼠标监控

### 6.1 使用 MouseDispatcher

```python
from rx_rust import MouseDispatcher

# 构造分发器
mouse = MouseDispatcher(
    backend="auto",
    interval=0.05,
    filter_self=True
)

# 生命周期
mouse.start()
mouse.stop()

# 使用上下文管理器
with MouseDispatcher() as mouse:
    mouse.subscribe(callback)
```

### 6.2 使用 MouseSubject

```python
from rx_rust import MouseSubject

with MouseSubject(backend="polling") as ms:
    # 订阅移动事件
    ms.subscribe(on_next=lambda e: print(f"移动: ({e.x}, {e.y})"))
```

### 6.3 过滤鼠标事件

```python
from rx_rust import from_mouse, MouseEventType, ops

obs, mouse = from_mouse()

# 只监听左键点击
obs.pipe(
    ops.filter(lambda e: e.event_type == MouseEventType.LEFT_UP)
).subscribe(on_next=lambda e: print(f"左键释放于: ({e.x}, {e.y})"))

# 只监听滚轮
obs.pipe(
    ops.filter(lambda e: e.event_type == MouseEventType.SCROLL)
).subscribe(on_next=lambda e: print(f"滚轮: delta={e.delta}"))
```

---

## 7. 响应式操作符

### 7.1 write_to_keyboard

将上游流中的数据写回键盘：

```python
from rx_rust import from_keyboard, write_to_keyboard
from rx_rust import Observable

# 创建键盘流
obs, kbd = from_keyboard()

# 创建写入操作符
write_op = write_to_keyboard(kbd)

# 应用到源流
result = write_op(source_stream)
result.subscribe(on_next=lambda e: print(f"写入: {e}"))

# 支持的输入类型:
# - str: 直接 type_text
# - int: key_code，按下+释放
# - dict: {"key": "A"} 或 {"text": "hello"}
# - tuple: (key_code, is_press)
```

### 7.2 write_to_mouse

将上游流中的数据写回鼠标：

```python
from rx_rust import from_mouse, write_to_mouse

obs, mouse = from_mouse()
write_op = write_to_mouse(mouse)

# 支持的输入类型:
# - dict: {"x": 100, "y": 200, "event": "move"}
# - tuple: (x, y, event_type)
# - MouseData
```

---

## 8. 模拟操作

### 8.1 KeyboardDispatcher 模拟方法

```python
kbd = KeyboardDispatcher(backend="polling")
kbd.start()

# 按下按键
kbd.press("A")      # 按下 A
kbd.release("A")    # 释放 A

# 敲击（按下+释放）
kbd.tap("Enter")    # 按下并释放 Enter

# 输入文本
kbd.type_text("Hello, World!")

# 组合键
kbd.hotkey("Ctrl", "A")   # Ctrl+A 全选
kbd.hotkey("Ctrl", "Shift", "S")  # Ctrl+Shift+S

# 支持的键名:
# 字母: "A"-"Z", "0"-"9"
# 功能键: "F1"-"F12"
# 控制: "Enter", "Esc", "Tab", "Space", "Backspace", "Delete"
# 方向: "Left", "Right", "Up", "Down"
# 修饰: "Shift", "Ctrl", "Alt", "Win"
kbd.stop()
```

### 8.2 MouseDispatcher 模拟方法

```python
mouse = MouseDispatcher(backend="polling")
mouse.start()

# 移动鼠标
mouse.move_to(500, 300)      # 移动到绝对坐标
mouse.move_relative(10, 10)   # 相对移动

# 点击
mouse.click("left")           # 左键单击
mouse.click("right")          # 右键单击
mouse.click("middle")         # 中键单击
mouse.double_click("left")    # 左键双击

# 滚动
mouse.scroll(120)             # 向上滚动 (正数)
mouse.scroll(-120)            # 向下滚动 (负数)

# 拖拽
mouse.drag(from_x=100, from_y=100, to_x=500, to_y=500, button="left")

mouse.stop()
```

---

## 9. 自我过滤机制

### 9.1 什么是自我过滤

当你调用 `kbd.type_text("A")` 模拟按键时，系统会捕获到这个按键事件。如果不加过滤，这个事件会再次触发你的订阅回调，导致无限循环。

### 9.2 自动过滤

默认启用 `filter_self=True`，会自动过滤模拟操作产生的事件：

```python
kbd = KeyboardDispatcher(filter_self=True)  # 默认值
kbd.start()

kbd.type_text("A")  # 这个 "A" 不会触发订阅回调
```

### 9.3 自定义过滤

使用 `self_filter` 参数传入自定义过滤函数：

```python
def custom_filter(kd):
    # 过滤所有 Shift 组合键
    return kd.modifiers & KeyModifier.SHIFT != 0

kbd = KeyboardDispatcher(
    filter_self=True,
    self_filter=custom_filter
)
kbd.start()
```

### 9.4 统计

```python
kbd.start()

kbd.type_text("test")
print(f"分发次数: {kbd.dispatch_count}")
print(f"过滤次数: {kbd.self_filtered_count}")
print(f"错误次数: {kbd.error_count}")

kbd.stop()
```

---

## 10. Observer 观察者

### 10.1 KeyObserver

声明式监听键盘事件：

```python
from rx_rust import KeyObserver, KeySubject

ks = KeySubject(backend="polling")

# 创建观察者
obs = KeyObserver(
    on_press=lambda kd: print(f"按下: {kd.key_name}"),
    on_release=lambda kd: print(f"释放: {kd.key_name}"),
    on_any=lambda kd: print(f"事件: {kd.event_type}"),
    on_error=lambda e: print(f"错误: {e}"),
    on_completed=lambda: print("完成")
)

# 订阅
obs.subscribe(ks)

# 或使用 attach
obs.attach(ks)

# 取消订阅
obs.unsubscribe()
print(f"是否已订阅: {obs.is_subscribed}")  # False

# 上下文管理器
with KeyObserver(on_press=handler) as obs:
    obs.attach(ks)
    # ...
# 自动取消订阅
```

### 10.2 MouseObserver

```python
from rx_rust import MouseObserver, MouseSubject

ms = MouseSubject(backend="polling")

# 创建观察者
obs = MouseObserver(
    on_move=lambda md: print(f"移动: ({md.x}, {md.y})"),
    on_click=lambda md: print(f"点击: {md.button}"),
    on_scroll=lambda md: print(f"滚轮: delta={md.delta}"),
    on_drag=lambda md: print(f"拖拽: ({md.x}, {md.y})"),
    on_left_down=lambda md: print("左键按下"),
    on_left_up=lambda md: print("左键释放"),
    on_right_down=lambda md: print("右键按下"),
    on_right_up=lambda md: print("右键释放"),
    on_middle_down=lambda md: print("中键按下"),
    on_middle_up=lambda md: print("中键释放"),
    on_any=lambda md: print(f"事件: {md.event_type}")
)

obs.attach(ms)
```

---

## 11. 完整示例

### 11.1 全局快捷键监听器

```python
from rx_rust import from_keyboard, KeySubject, KeyModifier, ops

# 创建键盘主题
ks = KeySubject(backend="polling")

# 监听 Ctrl+Shift+P (打开设置)
ks.pipe(
    ops.filter(lambda e: e.is_press),
    ops.filter(lambda e: e.key_name == "P"),
    ops.filter(lambda e: e.modifiers == (KeyModifier.CTRL | KeyModifier.SHIFT))
).subscribe(on_next=lambda e: print("打开设置!"))

print("监听中... 按 Ctrl+Shift+P 触发")
print("按 Enter 退出")
input()

ks.stop()
```

### 11.2 鼠标点击计数器

```python
from rx_rust import MouseSubject, MouseEventType, ops

ms = MouseSubject(backend="polling")

click_count = [0]

ms.pipe(
    ops.filter(lambda e: e.event_type == MouseEventType.LEFT_UP)
).subscribe(on_next=lambda e: {
    click_count.__setitem__(0, click_count[0] + 1),
    print(f"左键点击次数: {click_count[0]}")
})

print("点击鼠标左键计数...")
input()
ms.stop()
```

### 11.3 自动化脚本

```python
from rx_rust import KeyboardDispatcher, MouseDispatcher, sleep

kbd = KeyboardDispatcher(backend="polling")
mouse = MouseDispatcher(backend="polling")

kbd.start()
mouse.start()

# 打开记事本
kbd.hotkey("Win", "R")
sleep(0.5)
kbd.type_text("notepad")
kbd.tap("Enter")
sleep(1)

# 输入文本
kbd.type_text("Hello from rx_rust!")
kbd.tap("Enter")
kbd.type_text("This is automated input.")

# 保存文件
kbd.hotkey("Ctrl", "S")
sleep(0.5)
kbd.type_text("test.txt")
kbd.tap("Enter")

kbd.stop()
mouse.stop()
```

### 11.4 响应式操作符链

```python
from rx_rust import from_keyboard, from_mouse, ops

# 组合键盘和鼠标事件
key_obs, kbd = from_keyboard()
mouse_obs, mouse = from_mouse()

# 键盘事件流
key_stream = key_obs.pipe(
    ops.filter(lambda e: e.is_press),
    ops.map(lambda e: f"KEY:{e.key_name}")
)

# 鼠标事件流
mouse_stream = mouse_obs.pipe(
    ops.filter(lambda e: e.event_type == 0),  # MOVE
    ops.map(lambda e: f"MOUSE:({e.x},{e.y})")
)

# 合并两个流
combined = key_stream.pipe(
    ops.merge_with(mouse_stream)
)

combined.subscribe(on_next=lambda msg: print(msg))

kbd.stop()
mouse.stop()
```

---

## 12. API 参考

### 12.1 枚举类型

#### KeyEventType
```python
class KeyEventType(IntEnum):
    KEY_DOWN = 0   # 按键按下
    KEY_UP = 1     # 按键释放
    KEY_HOLD = 2    # 按键持续按住
```

#### MouseEventType
```python
class MouseEventType(IntEnum):
    MOVE = 0        # 鼠标移动
    LEFT_DOWN = 1   # 左键按下
    LEFT_UP = 2     # 左键释放
    RIGHT_DOWN = 3   # 右键按下
    RIGHT_UP = 4     # 右键释放
    MIDDLE_DOWN = 5  # 中键按下
    MIDDLE_UP = 6    # 中键释放
    SCROLL = 7       # 滚轮滚动
    DRAG = 8         # 鼠标拖拽
```

#### KeyModifier
```python
class KeyModifier(IntFlag):
    NONE = 0        # 无修饰键
    SHIFT = 1        # Shift 键
    CTRL = 2         # Ctrl 键
    ALT = 4          # Alt 键
    WIN = 8          # Windows 键
    CAPSLOCK = 16   # Caps Lock 状态
```

### 12.2 数据结构

#### KeyData
```python
@dataclass
class KeyData:
    key_code: int           # 虚拟键码 (VK_*)
    key_name: str           # 键名 ("A", "Enter", "F1"...)
    is_press: bool          # True=按下, False=释放
    event_type: KeyEventType # 事件类型
    modifiers: int           # 修饰键位组合
    timestamp: int           # Unix 毫秒时间戳
    sequence: int           # 全局单调递增序号
    window_title: str        # 窗口标题（可选）

    def to_dict() -> Dict[str, Any]: ...
    @classmethod
    def from_dict(cls, data: Dict) -> KeyData: ...
    def to_json() -> str: ...
    @classmethod
    def from_json(cls, s: str) -> KeyData: ...
    def to_pickle() -> bytes: ...
    @classmethod
    def from_pickle(cls, b: bytes, trusted: bool = False) -> KeyData: ...
```

#### MouseData
```python
@dataclass
class MouseData:
    x: int                  # X 坐标
    y: int                  # Y 坐标
    event_type: MouseEventType  # 事件类型
    button: str              # 按钮 ("left"/"right"/"middle"/"none")
    delta: int               # 滚轮增量
    timestamp: int           # Unix 毫秒时间戳
    sequence: int           # 全局单调递增序号

    def to_dict() -> Dict[str, Any]: ...
    @classmethod
    def from_dict(cls, data: Dict) -> MouseData: ...
    def to_json() -> str: ...
    @classmethod
    def from_json(cls, s: str) -> MouseData: ...
    def to_pickle() -> bytes: ...
    @classmethod
    def from_pickle(cls, b: bytes, trusted: bool = False) -> MouseData: ...
```

### 12.3 分发器

#### KeyboardDispatcher
```python
class KeyboardDispatcher:
    def __init__(
        self,
        backend: str = "auto",
        interval: float = 0.05,
        filter_self: bool = True,
        self_filter: Optional[Callable[[KeyData], bool]] = None,
        auto_start: bool = False
    ): ...

    # 属性
    @property
    def subject(self) -> Any: ...       # PublishSubject
    @property
    def backend_name(self) -> str: ...  # "win32" | "polling"
    @property
    def dispatch_count(self) -> int: ...
    @property
    def error_count(self) -> int: ...
    @property
    def self_filtered_count(self) -> int: ...
    @property
    def is_running(self) -> bool: ...

    # 生命周期
    def start() -> None: ...
    def stop() -> None: ...
    def subscribe(on_next: Callable) -> Subscription: ...

    # 模拟操作
    def press(key: str) -> None: ...
    def release(key: str) -> None: ...
    def tap(key: str) -> None: ...
    def type_text(text: str) -> None: ...
    def hotkey(*keys: str) -> None: ...
```

#### MouseDispatcher
```python
class MouseDispatcher:
    def __init__(
        self,
        backend: str = "auto",
        interval: float = 0.05,
        filter_self: bool = True,
        self_filter: Optional[Callable[[MouseData], bool]] = None,
        auto_start: bool = False
    ): ...

    # 属性
    @property
    def subject(self) -> Any: ...
    @property
    def backend_name(self) -> str: ...
    @property
    def dispatch_count(self) -> int: ...
    @property
    def error_count(self) -> int: ...
    @property
    def self_filtered_count(self) -> int: ...
    @property
    def is_running(self) -> bool: ...

    # 生命周期
    def start() -> None: ...
    def stop() -> None: ...
    def subscribe(on_next: Callable) -> Subscription: ...

    # 模拟操作
    def move_to(x: int, y: int) -> None: ...
    def move_relative(dx: int, dy: int) -> None: ...
    def click(button: str = "left") -> None: ...
    def double_click(button: str = "left") -> None: ...
    def scroll(delta: int = 120) -> None: ...
    def drag(from_x: int, from_y: int, to_x: int, to_y: int, button: str = "left") -> None: ...
```

### 12.4 主题

#### KeySubject
```python
class KeySubject:
    def __init__(
        self,
        backend: str = "auto",
        interval: float = 0.05,
        filter_self: bool = True,
        self_filter: Optional[Callable[[KeyData], bool]] = None
    ): ...

    @property
    def dispatcher(self) -> KeyboardDispatcher: ...
    @property
    def subject(self) -> Any: ...
    @property
    def backend_name(self) -> str: ...
    @property
    def dispatch_count(self) -> int: ...
    @property
    def self_filtered_count(self) -> int: ...
    @property
    def is_running(self) -> bool: ...

    def subscribe(on_next: Callable) -> Subscription: ...
    def start() -> None: ...
    def stop() -> None: ...
    def press(key: str) -> None: ...
    def release(key: str) -> None: ...
    def tap(key: str) -> None: ...
    def type_text(text: str) -> None: ...
    def hotkey(*keys: str) -> None: ...
```

#### MouseSubject
```python
class MouseSubject:
    def __init__(
        self,
        backend: str = "auto",
        interval: float = 0.05,
        filter_self: bool = True,
        self_filter: Optional[Callable[[MouseData], bool]] = None
    ): ...

    @property
    def dispatcher(self) -> MouseDispatcher: ...
    @property
    def subject(self) -> Any: ...
    @property
    def backend_name(self) -> str: ...
    @property
    def dispatch_count(self) -> int: ...
    @property
    def self_filtered_count(self) -> int: ...
    @property
    def is_running(self) -> bool: ...

    def subscribe(on_next: Callable) -> Subscription: ...
    def start() -> None: ...
    def stop() -> None: ...
    def move_to(x: int, y: int) -> None: ...
    def move_relative(dx: int, dy: int) -> None: ...
    def click(button: str = "left") -> None: ...
    def double_click(button: str = "left") -> None: ...
    def scroll(delta: int = 120) -> None: ...
    def drag(from_x: int, from_y: int, to_x: int, to_y: int, button: str = "left") -> None: ...
```

### 12.5 观察者

#### KeyObserver
```python
class KeyObserver:
    def __init__(
        self,
        on_press: Optional[Callable[[KeyData], Any]] = None,
        on_release: Optional[Callable[[KeyData], Any]] = None,
        on_hold: Optional[Callable[[KeyData], Any]] = None,
        on_any: Optional[Callable[[KeyData], Any]] = None,
        on_hotkey: Optional[Callable[[KeyData], Any]] = None,
        on_error: Optional[Callable[[Exception], Any]] = None,
        on_completed: Optional[Callable[[], Any]] = None
    ): ...

    def subscribe(self, observable: Any) -> Subscription: ...
    def attach(self, subject_or_dispatcher: Any) -> KeyObserver: ...
    def unsubscribe() -> None: ...

    @property
    def is_subscribed(self) -> bool: ...
```

#### MouseObserver
```python
class MouseObserver:
    def __init__(
        self,
        on_move: Optional[Callable[[MouseData], Any]] = None,
        on_left_down: Optional[Callable[[MouseData], Any]] = None,
        on_left_up: Optional[Callable[[MouseData], Any]] = None,
        on_right_down: Optional[Callable[[MouseData], Any]] = None,
        on_right_up: Optional[Callable[[MouseData], Any]] = None,
        on_middle_down: Optional[Callable[[MouseData], Any]] = None,
        on_middle_up: Optional[Callable[[MouseData], Any]] = None,
        on_scroll: Optional[Callable[[MouseData], Any]] = None,
        on_drag: Optional[Callable[[MouseData], Any]] = None,
        on_click: Optional[Callable[[MouseData], Any]] = None,
        on_any: Optional[Callable[[MouseData], Any]] = None,
        on_error: Optional[Callable[[Exception], Any]] = None,
        on_completed: Optional[Callable[[], Any]] = None
    ): ...

    def subscribe(self, observable: Any) -> Subscription: ...
    def attach(self, subject_or_dispatcher: Any) -> MouseObserver: ...
    def unsubscribe() -> None: ...

    @property
    def is_subscribed(self) -> bool: ...
```

### 12.6 工厂函数

#### from_keyboard
```python
def from_keyboard(
    backend: str = "auto",
    interval: float = 0.05,
    filter_self: bool = True,
    auto_start: bool = True,
    self_filter: Optional[Callable[[KeyData], bool]] = None
) -> Tuple[Any, KeyboardDispatcher]:
    """创建键盘事件流"""
```

#### from_mouse
```python
def from_mouse(
    backend: str = "auto",
    interval: float = 0.05,
    filter_self: bool = True,
    auto_start: bool = True,
    self_filter: Optional[Callable[[MouseData], bool]] = None
) -> Tuple[Any, MouseDispatcher]:
    """创建鼠标事件流"""
```

### 12.7 写入操作符

#### write_to_keyboard
```python
def write_to_keyboard(dispatcher: KeyboardDispatcher) -> Callable[[Observable], Observable]:
    """响应式操作符：把流内容写回键盘"""
```

#### write_to_mouse
```python
def write_to_mouse(dispatcher: MouseDispatcher) -> Callable[[Observable], Observable]:
    """响应式操作符：把流内容写回鼠标"""
```

---

## 13. 常见问题

### Q: 如何降低 CPU 占用？
A: 使用 `polling` 后端时，增加 `interval` 参数值（如 0.1 或 0.2）。

### Q: 为什么模拟输入没有生效？
A: 检查：
1. 是否有焦点窗口
2. 是否需要管理员权限（Windows Hook 模式）
3. `filter_self=True` 是否正确设置

### Q: 如何监听组合键？
A:
```python
ks.pipe(
    ops.filter(lambda e: e.is_press),
    ops.filter(lambda e: e.key_name == "A"),
    ops.filter(lambda e: e.modifiers & KeyModifier.CTRL)
).subscribe(on_next=lambda e: print("Ctrl+A 按下!"))
```

### Q: 如何区分按键的按下和释放？
A: 检查 `KeyData.is_press` 或 `KeyData.event_type`。

### Q: 非 Windows 平台如何使用？
A: 使用 `backend="polling"`。Hook 模式仅 Windows 支持。

---

*文档版本: 1.0.0*
*最后更新: 2026-06-15*
