"""
rx-rust.keyboard_mouse - 键盘鼠标监控与响应式分发

核心公共 API:
    KeyEventType(IntEnum):    键盘事件类型枚举
    MouseEventType(IntEnum):  鼠标事件类型枚举
    KeyData:                 键盘事件数据结构（含 event_type/window_title）
    MouseData:               鼠标事件数据结构
    KeyModifier(IntEnum):     键盘修饰键位标志
    KeyboardDispatcher:       键盘监控与分发器
        - subject 属性: 返回 PublishSubject
        - tap(key): 一次性按下+释放
        - self_filter 参数: 自定义过滤函数
        - self_filtered_count: 过滤统计
    MouseDispatcher:          鼠标监控与分发器
        - double_click(button): 双击
        - move_relative(dx, dy): 相对移动
    from_keyboard(...):       顶层工厂：返回 (Observable[KeyData], KeyboardDispatcher)
        - auto_start: 是否自动启动监控
        - self_filter: 自定义过滤函数
    from_mouse(...):          顶层工厂：返回 (Observable[MouseData], MouseDispatcher)
    write_to_keyboard(d):     响应式操作符：把流内容写回键盘输入
        - 支持 tuple/list (key_code, is_press) 输入
    write_to_mouse(d):        响应式操作符：把流内容写回鼠标操作
    KeyObserver:              按键事件路由观察者（on_press/on_release）
    MouseObserver:            鼠标事件路由观察者（on_move/on_click/on_scroll）

自我过滤机制 (self-filter):
    下游通过 Dispatcher.press/release/type_text/move_to 等写回输入时，
    Dispatcher 登记本次写入的事件签名。当系统再次通知键鼠变化时，
    命中签名的内容会被丢弃，从而避免"下游写回又触发自己"的死循环。
"""

from __future__ import annotations

import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import (
    Observable,
    _PyObservable,
    Subscription,
)


# ============================================================================
# 尝试加载 Rust 扩展
# ============================================================================

_USE_RUST = False

try:
    from . import rx_rust as _rust_mod

    # 验证 Rust 模块中有键鼠类型
    _HAS_KEYBOARD_MOUSE = hasattr(_rust_mod, "KeyEventType")
    _USE_RUST = _HAS_KEYBOARD_MOUSE
except (ImportError, AttributeError):
    _USE_RUST = False
    _HAS_KEYBOARD_MOUSE = False


# ============================================================================
# Rust 类型别名（如果有 Rust 扩展）
# ============================================================================

if _USE_RUST:
    KeyEventType = _rust_mod.KeyEventType
    MouseEventType = _rust_mod.MouseEventType
    KeyData = _rust_mod.KeyData
    MouseData = _rust_mod.MouseData
    KeyModifier = _rust_mod.KeyModifier
    KeyboardDispatcher = _rust_mod.KeyboardDispatcher
    MouseDispatcher = _rust_mod.MouseDispatcher
    KeySubject = _rust_mod.KeySubject
    MouseSubject = _rust_mod.MouseSubject
    KeyObserver = _rust_mod.KeyObserver
    MouseObserver = _rust_mod.MouseObserver
else:
    # Rust 扩展未安装时提供存根类型，避免导入错误
    from enum import IntEnum, IntFlag

    class KeyEventType(IntEnum):
        """键盘事件类型枚举（存根 - Rust 扩展未安装）"""
        KEY_DOWN = 0
        KEY_UP = 1
        KEY_HOLD = 2

    class MouseEventType(IntEnum):
        """鼠标事件类型枚举（存根 - Rust 扩展未安装）"""
        MOVE = 0
        LEFT_DOWN = 1
        LEFT_UP = 2
        RIGHT_DOWN = 3
        RIGHT_UP = 4
        MIDDLE_DOWN = 5
        MIDDLE_UP = 6
        SCROLL = 7
        DRAG = 8

    class KeyModifier(IntFlag):
        """键盘修饰键标志（存根 - Rust 扩展未安装）"""
        NONE = 0
        SHIFT = 1
        CTRL = 2
        ALT = 4
        WIN = 8

    class KeyData:
        """键盘事件数据（存根 - Rust 扩展未安装）"""
        pass

    class MouseData:
        """鼠标事件数据（存根 - Rust 扩展未安装）"""
        pass

    class KeyboardDispatcher:
        """键盘分发器（存根 - Rust 扩展未安装）"""
        def __init__(self, *args, **kwargs):
            raise NotImplementedError(
                "键盘监控需要 Rust 扩展。请运行 'maturin develop' 或 'pip install .'"
            )

    class MouseDispatcher:
        """鼠标分发器（存根 - Rust 扩展未安装）"""
        def __init__(self, *args, **kwargs):
            raise NotImplementedError(
                "鼠标监控需要 Rust 扩展。请运行 'maturin develop' 或 'pip install .'"
            )

    class KeySubject:
        """键盘主题（存根 - Rust 扩展未安装）"""
        def __init__(self, *args, **kwargs):
            raise NotImplementedError(
                "键盘监控需要 Rust 扩展。请运行 'maturin develop' 或 'pip install .'"
            )

    class MouseSubject:
        """鼠标主题（存根 - Rust 扩展未安装）"""
        def __init__(self, *args, **kwargs):
            raise NotImplementedError(
                "鼠标监控需要 Rust 扩展。请运行 'maturin develop' 或 'pip install .'"
            )

    class KeyObserver:
        """键盘观察者（存根 - Rust 扩展未安装）"""
        def __init__(self, *args, **kwargs):
            raise NotImplementedError(
                "键盘监控需要 Rust 扩展。请运行 'maturin develop' 或 'pip install .'"
            )

    class MouseObserver:
        """鼠标观察者（存根 - Rust 扩展未安装）"""
        def __init__(self, *args, **kwargs):
            raise NotImplementedError(
                "鼠标监控需要 Rust 扩展。请运行 'maturin develop' 或 'pip install .'"
            )


# ============================================================================
# 顶层工厂函数
# ============================================================================

def from_keyboard(
    backend: str = "auto",
    interval: float = 0.05,
    filter_self: bool = True,
    auto_start: bool = True,
    self_filter: Optional[Callable[[Any], bool]] = None,
) -> Tuple[Any, Any]:
    """创建键盘事件流。

    参数:
        backend: 后端类型 ("auto", "hook", "polling")
        interval: polling 模式下的轮询间隔（秒）
        filter_self: 是否启用自我过滤（过滤自己产生的输入事件）
        auto_start: 是否自动启动监控（默认 True）
        self_filter: 自定义过滤函数，返回 True 表示过滤该事件

    返回:
        Tuple[Observable[KeyData], KeyboardDispatcher]:
        - Observable 用于订阅事件
        - Dispatcher 用于控制生命周期和模拟输入

    示例:
        >>> obs, disp = from_keyboard()
        >>> obs.subscribe(on_next=lambda e: print(f"按键: {e.key_name}"))
        >>> disp.start()
        >>> # ... 之后
        >>> disp.stop()
    """
    if _USE_RUST:
        try:
            obs, disp = _rust_mod.from_keyboard(
                backend=backend,
                interval=interval,
                filter_self=filter_self,
                auto_start=auto_start,
                self_filter=self_filter,
            )
            return obs, disp
        except Exception:
            pass

    # Python 回退
    raise NotImplementedError(
        "键盘监控的纯 Python 回退尚未实现，请使用 Rust 扩展 "
        "（maturin develop 或 pip install .）"
    )


def from_mouse(
    backend: str = "auto",
    interval: float = 0.05,
    filter_self: bool = True,
    auto_start: bool = True,
    self_filter: Optional[Callable[[Any], bool]] = None,
) -> Tuple[Any, Any]:
    """创建鼠标事件流。

    参数:
        backend: 后端类型 ("auto", "hook", "polling")
        interval: polling 模式下的轮询间隔（秒）
        filter_self: 是否启用自我过滤（过滤自己产生的输入事件）
        auto_start: 是否自动启动监控（默认 True）
        self_filter: 自定义过滤函数，返回 True 表示过滤该事件

    返回:
        Tuple[Observable[MouseData], MouseDispatcher]:
        - Observable 用于订阅事件
        - Dispatcher 用于控制生命周期和模拟输入

    示例:
        >>> obs, disp = from_mouse()
        >>> obs.subscribe(on_next=lambda e: print(f"鼠标: ({e.x}, {e.y})"))
        >>> disp.start()
    """
    if _USE_RUST:
        try:
            obs, disp = _rust_mod.from_mouse(
                backend=backend,
                interval=interval,
                filter_self=filter_self,
                auto_start=auto_start,
                self_filter=self_filter,
            )
            return obs, disp
        except Exception:
            pass

    raise NotImplementedError(
        "鼠标监控的纯 Python 回退尚未实现，请使用 Rust 扩展"
    )


# ============================================================================
# 响应式操作符
# ============================================================================

def write_to_keyboard(
    dispatcher: Any,
) -> Callable[[Any], Any]:
    """响应式操作符：把上游每一项写回键盘输入，并把原始项继续下发。

    上游可接受:
        str   → 直接 type_text
        int   → 作为 key_code，按下+释放
        tuple/list → (key_code, is_press) 格式，is_press=True 按下，False 释放
        dict  → {"key": "A"} 或 {"text": "hello"} 或 {"key_code": 65}
        KeyData → 使用 key_code 和 is_press

    示例:
        >>> obs, disp = from_keyboard()
        >>> text_stream.pipe(write_to_keyboard(disp)).subscribe()
    """
    if _USE_RUST:
        try:
            return _rust_mod.write_to_keyboard(dispatcher)
        except Exception:
            pass

    def _make_op(source: Any) -> Observable:
        def _subscribe_func(observer: Callable) -> Subscription:
            def _on_next(item: Any):
                try:
                    # 根据类型写回
                    if isinstance(item, str):
                        dispatcher.type_text(item)
                    elif isinstance(item, (tuple, list)) and len(item) >= 2:
                        # 新增：支持 tuple/list (key_code, is_press)
                        key_code, is_press = item[0], item[1]
                        if is_press:
                            dispatcher.press(f"VK_{key_code}")
                        else:
                            dispatcher.release(f"VK_{key_code}")
                    elif isinstance(item, int):
                        code = item
                        dispatcher.press(f"VK_0x{code:02X}")
                        dispatcher.release(f"VK_0x{code:02X}")
                    elif isinstance(item, dict):
                        if "text" in item:
                            dispatcher.type_text(item["text"])
                        elif "key" in item:
                            k = item["key"]
                            is_press = item.get("is_press", True)
                            if is_press:
                                dispatcher.press(k)
                            else:
                                dispatcher.release(k)
                        elif "key_code" in item:
                            code = item["key_code"]
                            is_press = item.get("is_press", True)
                            if is_press:
                                dispatcher.press(f"VK_0x{code:02X}")
                            else:
                                dispatcher.release(f"VK_0x{code:02X}")
                    # 透传
                    if callable(observer):
                        observer(item)
                except Exception:
                    pass

            inner = getattr(source, "_inner", source)
            if hasattr(inner, "subscribe"):
                return inner.subscribe(_on_next)
            return Subscription()

        return Observable(_PyObservable(_subscribe_func))

    return _make_op


def write_to_mouse(
    dispatcher: Any,
) -> Callable[[Any], Any]:
    """响应式操作符：把上游每一项写回鼠标操作，并把原始项继续下发。

    上游可接受:
        dict        → {"x": 100, "y": 200, "event": "move"|"click"|"scroll"}
        tuple/list  → (x, y, event_type)  event_type: 0=move,1=click,2=scroll
        MouseData   → 直接使用字段值

    示例:
        >>> obs, disp = from_mouse()
        >>> coord_stream.pipe(write_to_mouse(disp)).subscribe()
    """
    if _USE_RUST:
        try:
            return _rust_mod.write_to_mouse(dispatcher)
        except Exception:
            pass

    def _make_op(source: Any) -> Observable:
        def _subscribe_func(observer: Callable) -> Subscription:
            def _on_next(item: Any):
                try:
                    if isinstance(item, dict):
                        x = item.get("x", 0)
                        y = item.get("y", 0)
                        event = item.get("event", "move")
                        delta = item.get("delta", 0)

                        if event == "move":
                            dispatcher.move_to(x, y)
                        elif event == "click":
                            button = item.get("button", "left")
                            dispatcher.click(button)
                        elif event == "scroll":
                            dispatcher.scroll(delta)
                        elif event == "drag":
                            dispatcher.drag(
                                item.get("from_x", 0),
                                item.get("from_y", 0),
                                x, y,
                            )

                    elif isinstance(item, (tuple, list)) and len(item) >= 2:
                        x, y = item[0], item[1]
                        event_type = item[2] if len(item) > 2 else 0
                        if event_type == 0:
                            dispatcher.move_to(x, y)
                        elif event_type == 1:
                            dispatcher.click("left")
                        elif event_type == 2:
                            dispatcher.scroll(y)  # y 作为 delta

                    # 透传
                    if callable(observer):
                        observer(item)
                except Exception:
                    pass

            inner = getattr(source, "_inner", source)
            if hasattr(inner, "subscribe"):
                return inner.subscribe(_on_next)
            return Subscription()

        return Observable(_PyObservable(_subscribe_func))

    return _make_op


# ============================================================================
# __all__
# ============================================================================

__all__ = [
    "KeyEventType",
    "MouseEventType",
    "KeyData",
    "MouseData",
    "KeyModifier",
    "KeyboardDispatcher",
    "MouseDispatcher",
    "KeySubject",
    "MouseSubject",
    "KeyObserver",
    "MouseObserver",
    "from_keyboard",
    "from_mouse",
    "write_to_keyboard",
    "write_to_mouse",
]
