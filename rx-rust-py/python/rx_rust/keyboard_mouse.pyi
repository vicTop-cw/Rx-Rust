"""
rx_rust.keyboard_mouse - 键盘鼠标监控与模拟模块的类型存根

本文件提供完整的类型提示和文档字符串，支持 IDE 自动补全和类型检查。
"""

from __future__ import annotations

from enum import IntEnum, IntFlag
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

# =============================================================================
# 类型别名
# =============================================================================

T = TypeVar("T")
R = TypeVar("R")
OnNext = Callable[[T], Any]
OnError = Callable[[Exception], Any]
OnCompleted = Callable[[], Any]
SelfFilterKey = Callable[[KeyData], bool]
SelfFilterMouse = Callable[[MouseData], bool]

# =============================================================================
# 枚举类型
# =============================================================================


class KeyEventType(IntEnum):
    """键盘事件类型枚举。

    标识键盘按键事件的性质。

    成员:
        KEY_DOWN (0): 按键按下
        KEY_UP (1): 按键释放
        KEY_HOLD (2): 按键持续按住（仅 Polling 后端产生）
    """

    KEY_DOWN: int
    KEY_UP: int
    KEY_HOLD: int


class MouseEventType(IntEnum):
    """鼠标事件类型枚举。

    标识鼠标操作事件的性质。

    成员:
        MOVE (0): 鼠标移动
        LEFT_DOWN (1): 左键按下
        LEFT_UP (2): 左键释放
        RIGHT_DOWN (3): 右键按下
        RIGHT_UP (4): 右键释放
        MIDDLE_DOWN (5): 中键按下
        MIDDLE_UP (6): 中键释放
        SCROLL (7): 滚轮滚动
        DRAG (8): 鼠标拖拽
    """

    MOVE: int
    LEFT_DOWN: int
    LEFT_UP: int
    RIGHT_DOWN: int
    RIGHT_UP: int
    MIDDLE_DOWN: int
    MIDDLE_UP: int
    SCROLL: int
    DRAG: int


class KeyModifier(IntFlag):
    """键盘修饰键位标志（位标志组合）。

    使用位运算组合多个修饰键:
        >>> mod = KeyModifier.SHIFT | KeyModifier.CTRL
        >>> bool(mod & KeyModifier.SHIFT)
        True

    成员:
        NONE (0): 无修饰键
        SHIFT (1): Shift 键
        CTRL (2): Ctrl 键
        ALT (4): Alt 键
        WIN (8): Windows 键
        CAPSLOCK (16): Caps Lock 状态
        LSHIFT (32): 左 Shift 键
        RSHIFT (64): 右 Shift 键
        LCTRL (128): 左 Ctrl 键
        RCTRL (256): 右 Ctrl 键
    """

    NONE: int
    SHIFT: int
    CTRL: int
    ALT: int
    WIN: int
    CAPSLOCK: int
    LSHIFT: int
    RSHIFT: int
    LCTRL: int
    RCTRL: int


# =============================================================================
# 数据结构
# =============================================================================


class KeyData:
    """结构化键盘事件数据。

    字段:
        key_code: 虚拟键码（Windows VK_* 值）
        key_name: 键名（如 "A", "ENTER", "F1"）
        is_press: True=按下, False=释放
        event_type: KeyEventType（按下→KEY_DOWN, 释放→KEY_UP）
        modifiers: 修饰键位组合（位标志）
        timestamp: 事件时间戳（Unix 毫秒）
        sequence: 全局单调递增序号
        window_title: 前台窗口标题（可选）
    """

    key_code: int
    key_name: str
    is_press: bool
    event_type: KeyEventType
    modifiers: int
    timestamp: int
    sequence: int
    window_title: Optional[str]

    def __init__(
        self,
        key_code: int,
        key_name: str = ...,
        is_press: bool = ...,
        event_type: Optional[KeyEventType] = ...,
        modifiers: int = ...,
        timestamp: Optional[int] = ...,
        sequence: Optional[int] = ...,
        window_title: Optional[str] = ...,
    ) -> None: ...
    @classmethod
    def now(
        cls,
        key_code: int,
        is_press: bool = ...,
        modifiers: Optional[int] = ...,
        window_title: Optional[str] = ...,
    ) -> KeyData: ...
    def to_dict(self) -> Dict[str, Any]: ...
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> KeyData: ...
    def to_json(self) -> str: ...
    @classmethod
    def from_json(cls, s: str) -> KeyData: ...
    def to_pickle(self) -> bytes: ...
    @classmethod
    def from_pickle(cls, b: bytes, trusted: bool = ...) -> KeyData: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...


class MouseData:
    """结构化鼠标事件数据。

    字段:
        x: 屏幕坐标 X（像素）
        y: 屏幕坐标 Y（像素）
        event_type: MouseEventType
        button: 按钮名 ("left"/"right"/"middle"/"none")
        delta: 滚轮增量（仅 SCROLL 事件有效）
        timestamp: 事件时间戳（Unix 毫秒）
        sequence: 全局单调递增序号
    """

    x: int
    y: int
    event_type: MouseEventType
    button: str
    delta: int
    timestamp: int
    sequence: int

    def __init__(
        self,
        x: int = ...,
        y: int = ...,
        event_type: MouseEventType = ...,
        button: str = ...,
        delta: int = ...,
        timestamp: Optional[int] = ...,
        sequence: Optional[int] = ...,
    ) -> None: ...
    @classmethod
    def now(
        cls,
        x: int = ...,
        y: int = ...,
        event_type: MouseEventType = ...,
        button: str = ...,
        delta: int = ...,
    ) -> MouseData: ...
    def to_dict(self) -> Dict[str, Any]: ...
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> MouseData: ...
    def to_json(self) -> str: ...
    @classmethod
    def from_json(cls, s: str) -> MouseData: ...
    def to_pickle(self) -> bytes: ...
    @classmethod
    def from_pickle(cls, b: bytes, trusted: bool = ...) -> MouseData: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...


# =============================================================================
# 订阅句柄
# =============================================================================


class Subscription:
    """订阅句柄，用于取消订阅。"""

    def dispose(self) -> None: ...
    def is_disposed(self) -> bool: ...


# =============================================================================
# 分发器
# =============================================================================


class KeyboardDispatcher:
    """键盘事件监控与分发器。

    使用 Windows Hook 或 Polling 模式监控全局键盘事件，
    并通过 PublishSubject 分发给订阅者。

    参数:
        backend: 后端选择 "auto" | "win32" | "polling"
        interval: Polling 模式的检查间隔（秒）
        filter_self: 是否启用自我过滤（默认 True）
        self_filter: 自定义过滤函数，返回 True 的事件将被丢弃
        auto_start: 是否自动启动（默认 False）

    示例:
        >>> kbd = KeyboardDispatcher(backend="polling")
        >>> kbd.start()
        >>> kbd.subscribe(lambda e: print(f"按键: {e.key_name}"))
        >>> kbd.type_text("hello")
        >>> kbd.stop()
    """

    backend_name: str
    dispatch_count: int
    error_count: int
    self_filtered_count: int
    is_running: bool

    def __init__(
        self,
        backend: str = ...,
        interval: float = ...,
        filter_self: bool = ...,
        self_filter: Optional[SelfFilterKey] = ...,
        self_filter_cap: int = ...,
    ) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def subscribe(self, on_next: OnNext[KeyData]) -> Subscription: ...
    @property
    def subject(self) -> Any: ...

    # 模拟操作
    def press(self, key: str) -> None:
        """按下指定的键。

        参数:
            key: 键名（如 "A", "Enter", "F1"）或键码

        示例:
            >>> kbd.press("A")
            >>> kbd.press(65)  # VK_A
        """
        ...
    def release(self, key: str) -> None:
        """释放指定的键。

        参数:
            key: 键名

        示例:
            >>> kbd.release("A")
        """
        ...
    def tap(self, key: str) -> None:
        """敲击（按下+释放）一个键。

        参数:
            key: 键名

        示例:
            >>> kbd.tap("Enter")
        """
        ...
    def type_text(self, text: str) -> None:
        """模拟输入文本。

        参数:
            text: 要输入的文本

        示例:
            >>> kbd.type_text("Hello, World!")
        """
        ...
    def hotkey(self, *keys: str) -> None:
        """组合键：同时按下全部，然后依次释放。

        参数:
            *keys: 键名列表

        示例:
            >>> kbd.hotkey("Ctrl", "A")  # 全选
            >>> kbd.hotkey("Ctrl", "Shift", "S")  # 另存为
        """
        ...

    def __enter__(self) -> KeyboardDispatcher: ...
    def __exit__(self, *args: Any) -> None: ...


class MouseDispatcher:
    """鼠标事件监控与分发器。

    使用 Windows Hook 或 Polling 模式监控全局鼠标事件，
    并通过 PublishSubject 分发给订阅者。

    参数:
        backend: 后端选择 "auto" | "win32" | "polling"
        interval: Polling 模式的检查间隔（秒）
        filter_self: 是否启用自我过滤（默认 True）
        self_filter: 自定义过滤函数，返回 True 的事件将被丢弃
        auto_start: 是否自动启动（默认 False）

    示例:
        >>> mouse = MouseDispatcher(backend="polling")
        >>> mouse.start()
        >>> mouse.subscribe(lambda e: print(f"鼠标: ({e.x}, {e.y})"))
        >>> mouse.move_to(500, 300)
        >>> mouse.stop()
    """

    backend_name: str
    dispatch_count: int
    error_count: int
    self_filtered_count: int
    is_running: bool

    def __init__(
        self,
        backend: str = ...,
        interval: float = ...,
        filter_self: bool = ...,
        self_filter: Optional[SelfFilterMouse] = ...,
        self_filter_cap: int = ...,
    ) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def subscribe(self, on_next: OnNext[MouseData]) -> Subscription: ...
    @property
    def subject(self) -> Any: ...

    # 模拟操作
    def move_to(self, x: int, y: int) -> None:
        """移动鼠标到指定屏幕坐标。

        参数:
            x: X 坐标（像素）
            y: Y 坐标（像素）

        示例:
            >>> mouse.move_to(500, 300)
        """
        ...
    def move_relative(self, dx: int, dy: int) -> None:
        """相对移动鼠标。

        参数:
            dx: X 方向增量
            dy: Y 方向增量

        示例:
            >>> mouse.move_relative(10, 10)
        """
        ...
    def click(self, button: str = ...) -> None:
        """鼠标单击。

        参数:
            button: 按钮 ("left" | "right" | "middle")

        示例:
            >>> mouse.click("left")
            >>> mouse.click("right")
        """
        ...
    def double_click(self, button: str = ...) -> None:
        """鼠标双击。

        参数:
            button: 按钮 ("left" | "right" | "middle")

        示例:
            >>> mouse.double_click("left")
        """
        ...
    def scroll(self, delta: int = ...) -> None:
        """鼠标滚轮滚动。

        参数:
            delta: 滚动量（正数向上，负数向下，默认 120）

        示例:
            >>> mouse.scroll(120)   # 向上
            >>> mouse.scroll(-120)  # 向下
        """
        ...
    def drag(
        self,
        from_x: int,
        from_y: int,
        to_x: int,
        to_y: int,
        button: str = ...,
    ) -> None:
        """鼠标拖拽：从起点移动到终点。

        参数:
            from_x: 起点 X 坐标
            from_y: 起点 Y 坐标
            to_x: 终点 X 坐标
            to_y: 终点 Y 坐标
            button: 按钮 ("left" | "right" | "middle")

        示例:
            >>> mouse.drag(100, 100, 500, 500, "left")
        """
        ...

    def __enter__(self) -> MouseDispatcher: ...
    def __exit__(self, *args: Any) -> None: ...


# =============================================================================
# 主题
# =============================================================================


class KeySubject:
    """带键盘监控能力的 Subject。

    自动管理 KeyboardDispatcher 的生命周期。

    参数:
        backend: 后端选择 "auto" | "win32" | "polling"
        interval: Polling 模式的检查间隔（秒）
        filter_self: 是否启用自我过滤
        self_filter: 自定义过滤函数

    示例:
        >>> with KeySubject(backend="polling") as ks:
        ...     ks.subscribe(lambda e: print(f"按键: {e.key_name}"))
        ...     ks.type_text("hello")
    """

    dispatcher: KeyboardDispatcher
    backend_name: str
    dispatch_count: int
    self_filtered_count: int
    is_running: bool

    def __init__(
        self,
        backend: str = ...,
        interval: float = ...,
        filter_self: bool = ...,
        self_filter: Optional[SelfFilterKey] = ...,
    ) -> None: ...
    def subscribe(self, on_next: OnNext[KeyData]) -> Subscription: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...

    # 模拟操作
    def press(self, key: str) -> None: ...
    def release(self, key: str) -> None: ...
    def tap(self, key: str) -> None: ...
    def type_text(self, text: str) -> None: ...
    def hotkey(self, *keys: str) -> None: ...

    def __enter__(self) -> KeySubject: ...
    def __exit__(self, *args: Any) -> None: ...
    def __del__(self) -> None: ...


class MouseSubject:
    """带鼠标监控能力的 Subject。

    自动管理 MouseDispatcher 的生命周期。

    参数:
        backend: 后端选择 "auto" | "win32" | "polling"
        interval: Polling 模式的检查间隔（秒）
        filter_self: 是否启用自我过滤
        self_filter: 自定义过滤函数

    示例:
        >>> with MouseSubject(backend="polling") as ms:
        ...     ms.subscribe(lambda e: print(f"鼠标: ({e.x}, {e.y})"))
        ...     ms.click("left")
    """

    dispatcher: MouseDispatcher
    backend_name: str
    dispatch_count: int
    self_filtered_count: int
    is_running: bool

    def __init__(
        self,
        backend: str = ...,
        interval: float = ...,
        filter_self: bool = ...,
        self_filter: Optional[SelfFilterMouse] = ...,
    ) -> None: ...
    def subscribe(self, on_next: OnNext[MouseData]) -> Subscription: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...

    # 模拟操作
    def move_to(self, x: int, y: int) -> None: ...
    def move_relative(self, dx: int, dy: int) -> None: ...
    def click(self, button: str = ...) -> None: ...
    def double_click(self, button: str = ...) -> None: ...
    def scroll(self, delta: int = ...) -> None: ...
    def drag(
        self,
        from_x: int,
        from_y: int,
        to_x: int,
        to_y: int,
        button: str = ...,
    ) -> None: ...

    def __enter__(self) -> MouseSubject: ...
    def __exit__(self, *args: Any) -> None: ...
    def __del__(self) -> None: ...


# =============================================================================
# 观察者
# =============================================================================


class KeyObserver:
    """声明式键盘事件观察者。

    按事件类型自动路由回调。

    参数:
        on_press: 按键按下回调
        on_release: 按键释放回调
        on_hold: 按键持续按住回调（仅 Polling 后端）
        on_any: 所有事件回调
        on_hotkey: 组合键回调
        on_error: 错误回调
        on_completed: 完成回调

    示例:
        >>> obs = KeyObserver(
        ...     on_press=lambda kd: print(f"按下: {kd.key_name}"),
        ...     on_release=lambda kd: print(f"释放: {kd.key_name}")
        ... )
        >>> obs.attach(key_subject)
    """

    is_subscribed: bool

    def __init__(
        self,
        on_press: Optional[OnNext[KeyData]] = ...,
        on_release: Optional[OnNext[KeyData]] = ...,
        on_hold: Optional[OnNext[KeyData]] = ...,
        on_any: Optional[OnNext[KeyData]] = ...,
        on_hotkey: Optional[OnNext[KeyData]] = ...,
        on_error: Optional[OnError] = ...,
        on_completed: Optional[OnCompleted] = ...,
    ) -> None: ...
    def subscribe(self, observable: Any) -> Subscription:
        """订阅 Observable/Subject/KeySubject。"""
        ...
    def attach(self, subject_or_dispatcher: Any) -> KeyObserver:
        """订阅并返回 self，方便链式调用。"""
        ...
    def unsubscribe(self) -> None:
        """取消订阅。"""
        ...

    def __enter__(self) -> KeyObserver: ...
    def __exit__(self, *args: Any) -> None: ...


class MouseObserver:
    """声明式鼠标事件观察者。

    按事件类型自动路由回调。

    参数:
        on_move: 鼠标移动回调
        on_click: 鼠标点击回调
        on_scroll: 滚轮滚动回调
        on_drag: 鼠标拖拽回调
        on_left_down: 左键按下回调
        on_left_up: 左键释放回调
        on_right_down: 右键按下回调
        on_right_up: 右键释放回调
        on_middle_down: 中键按下回调
        on_middle_up: 中键释放回调
        on_any: 所有事件回调
        on_error: 错误回调
        on_completed: 完成回调

    示例:
        >>> obs = MouseObserver(
        ...     on_move=lambda md: print(f"移动: ({md.x}, {md.y})"),
        ...     on_click=lambda md: print(f"点击: {md.button}")
        ... )
        >>> obs.attach(mouse_subject)
    """

    is_subscribed: bool

    def __init__(
        self,
        on_move: Optional[OnNext[MouseData]] = ...,
        on_click: Optional[OnNext[MouseData]] = ...,
        on_scroll: Optional[OnNext[MouseData]] = ...,
        on_drag: Optional[OnNext[MouseData]] = ...,
        on_left_down: Optional[OnNext[MouseData]] = ...,
        on_left_up: Optional[OnNext[MouseData]] = ...,
        on_right_down: Optional[OnNext[MouseData]] = ...,
        on_right_up: Optional[OnNext[MouseData]] = ...,
        on_middle_down: Optional[OnNext[MouseData]] = ...,
        on_middle_up: Optional[OnNext[MouseData]] = ...,
        on_any: Optional[OnNext[MouseData]] = ...,
        on_error: Optional[OnError] = ...,
        on_completed: Optional[OnCompleted] = ...,
    ) -> None: ...
    def subscribe(self, observable: Any) -> Subscription:
        """订阅 Observable/Subject/MouseSubject。"""
        ...
    def attach(self, subject_or_dispatcher: Any) -> MouseObserver:
        """订阅并返回 self，方便链式调用。"""
        ...
    def unsubscribe(self) -> None:
        """取消订阅。"""
        ...

    def __enter__(self) -> MouseObserver: ...
    def __exit__(self, *args: Any) -> None: ...


# =============================================================================
# 工厂函数
# =============================================================================


def from_keyboard(
    backend: str = ...,
    interval: float = ...,
    filter_self: bool = ...,
    auto_start: bool = ...,
    self_filter: Optional[SelfFilterKey] = ...,
) -> Tuple[Any, KeyboardDispatcher]:
    """顶层工厂函数：创建键盘事件流。

    返回 (Observable[KeyData], KeyboardDispatcher) 二元组。

    参数:
        backend: 后端选择 "auto" | "win32" | "polling"
        interval: Polling 模式的检查间隔（秒）
        filter_self: 是否启用自我过滤（默认 True）
        auto_start: 是否自动启动（默认 True）
        self_filter: 自定义过滤函数

    返回:
        Tuple[Observable[KeyData], KeyboardDispatcher]:
        - Observable 用于订阅事件
        - Dispatcher 用于控制生命周期和模拟输入

    示例:
        >>> obs, kbd = from_keyboard(backend="polling")
        >>> obs.subscribe(lambda e: print(f"按键: {e.key_name}"))
        >>> kbd.type_text("hello")
        >>> kbd.stop()
    """
    ...


def from_mouse(
    backend: str = ...,
    interval: float = ...,
    filter_self: bool = ...,
    auto_start: bool = ...,
    self_filter: Optional[SelfFilterMouse] = ...,
) -> Tuple[Any, MouseDispatcher]:
    """顶层工厂函数：创建鼠标事件流。

    返回 (Observable[MouseData], MouseDispatcher) 二元组。

    参数:
        backend: 后端选择 "auto" | "win32" | "polling"
        interval: Polling 模式的检查间隔（秒）
        filter_self: 是否启用自我过滤（默认 True）
        auto_start: 是否自动启动（默认 True）
        self_filter: 自定义过滤函数

    返回:
        Tuple[Observable[MouseData], MouseDispatcher]:
        - Observable 用于订阅事件
        - Dispatcher 用于控制生命周期和模拟输入

    示例:
        >>> obs, mouse = from_mouse(backend="polling")
        >>> obs.subscribe(lambda e: print(f"鼠标: ({e.x}, {e.y})"))
        >>> mouse.move_to(500, 300)
        >>> mouse.stop()
    """
    ...


# =============================================================================
# 写入操作符
# =============================================================================


def write_to_keyboard(dispatcher: KeyboardDispatcher) -> Callable[[Any], Any]:
    """响应式操作符：把流内容写回键盘。

    上游可接受:
        KeyData: 用 key_code/is_press 按下/释放
        str: 作为 type_text 输入
        int: 作为 key_code 按下+释放
        dict: {"key": "A"} 或 {"text": "hello"} 或 {"key_code": 65}
        tuple/list: (key_code, is_press)

    参数:
        dispatcher: KeyboardDispatcher 实例

    返回:
        可调用操作符，接收上游 Observable 并返回新的 Observable

    示例:
        >>> obs, kbd = from_keyboard()
        >>> text_stream.pipe(write_to_keyboard(kbd)).subscribe()
    """
    ...


def write_to_mouse(dispatcher: MouseDispatcher) -> Callable[[Any], Any]:
    """响应式操作符：把流内容写回鼠标。

    上游可接受:
        MouseData: 用 x/y/event_type 模拟鼠标操作
        dict: {"x":..,"y":..,"event":"move"|"click"|"scroll"|"drag"}
        tuple/list: (x, y, event_type)

    参数:
        dispatcher: MouseDispatcher 实例

    返回:
        可调用操作符，接收上游 Observable 并返回新的 Observable

    示例:
        >>> obs, mouse = from_mouse()
        >>> coord_stream.pipe(write_to_mouse(mouse)).subscribe()
    """
    ...


# =============================================================================
# 模块导出
# =============================================================================

__all__: List[str] = [
    # 枚举类型
    "KeyEventType",
    "MouseEventType",
    "KeyModifier",
    # 数据结构
    "KeyData",
    "MouseData",
    # 订阅
    "Subscription",
    # 分发器
    "KeyboardDispatcher",
    "MouseDispatcher",
    # 主题
    "KeySubject",
    "MouseSubject",
    # 观察者
    "KeyObserver",
    "MouseObserver",
    # 工厂函数
    "from_keyboard",
    "from_mouse",
    # 写入操作符
    "write_to_keyboard",
    "write_to_mouse",
]
