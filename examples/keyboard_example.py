"""
键盘监控示例

展示如何使用 KeyboardDispatcher 和 KeySubject 监听键盘事件。
"""

from rx_rust import (
    KeyboardDispatcher,
    KeySubject,
    KeyObserver,
    KeyEventType,
    KeyModifier,
    from_keyboard,
    ops,
)


def basic_keyboard_monitor():
    """基础键盘监控"""
    # 使用工厂函数创建键盘流
    obs, kbd = from_keyboard(backend="polling", auto_start=True)

    # 订阅所有按键事件
    obs.subscribe(on_next=lambda e: print(
        f"按键: {e.key_name} ({e.event_type}) | 修饰键: {e.modifiers}"
    ))

    print("监听键盘... 按 Ctrl+C 退出")
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        kbd.stop()


def keyboard_observer_example():
    """使用 KeyObserver 按事件类型路由"""
    ks = KeySubject(backend="polling")
    ks.start()

    # 创建 Observer，分别处理按下和释放
    observer = KeyObserver(
        on_press=lambda kd: print(f"[按下] {kd.key_name}"),
        on_release=lambda kd: print(f"[释放] {kd.key_name}"),
        on_any=lambda kd: print(f"[任意] 事件类型={kd.event_type}")
    )
    observer.attach(ks)

    print("监听键盘... 按 Ctrl+C 退出")
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ks.stop()


def hotkey_listener():
    """组合键监听示例"""
    ks = KeySubject(backend="polling")
    ks.start()

    # 监听 Ctrl+Shift+P
    ks.pipe(
        ops.filter(lambda e: e.is_press),
        ops.filter(lambda e: e.key_name == "P"),
        ops.filter(lambda e: (e.modifiers & (KeyModifier.CTRL | KeyModifier.SHIFT)) != 0)
    ).subscribe(on_next=lambda e: print("触发 Ctrl+Shift+P!"))

    print("监听组合键 Ctrl+Shift+P... 按 Ctrl+C 退出")
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ks.stop()


def keyboard_simulation():
    """键盘模拟示例"""
    kbd = KeyboardDispatcher(backend="polling")
    kbd.start()

    # 模拟输入
    kbd.type_text("Hello, World!")
    kbd.tap("Enter")
    kbd.hotkey("Ctrl", "A")  # 全选
    kbd.hotkey("Ctrl", "C")  # 复制

    print("模拟输入完成")
    kbd.stop()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == "monitor":
            basic_keyboard_monitor()
        elif mode == "observer":
            keyboard_observer_example()
        elif mode == "hotkey":
            hotkey_listener()
        elif mode == "simulate":
            keyboard_simulation()
        else:
            print(f"未知模式: {mode}")
    else:
        print("用法:")
        print("  python keyboard_example.py monitor   - 基础键盘监控")
        print("  python keyboard_example.py observer   - 使用 KeyObserver")
        print("  python keyboard_example.py hotkey     - 组合键监听")
        print("  python keyboard_example.py simulate   - 键盘模拟")