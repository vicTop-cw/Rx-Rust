"""
鼠标监控示例

展示如何使用 MouseDispatcher 和 MouseSubject 监听鼠标事件。
"""

from rx_rust import (
    MouseDispatcher,
    MouseSubject,
    MouseObserver,
    MouseEventType,
    from_mouse,
    ops,
)


def basic_mouse_monitor():
    """基础鼠标监控"""
    obs, mouse = from_mouse(backend="polling", auto_start=True)

    # 订阅所有鼠标事件
    obs.subscribe(on_next=lambda e: print(
        f"鼠标: ({e.x}, {e.y}) | 事件: {e.event_type} | 按钮: {e.button}"
    ))

    print("监听鼠标... 按 Ctrl+C 退出")
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        mouse.stop()


def mouse_observer_example():
    """使用 MouseObserver 按事件类型路由"""
    ms = MouseSubject(backend="polling")
    ms.start()

    # 创建 Observer，分别处理不同事件
    observer = MouseObserver(
        on_move=lambda md: print(f"移动: ({md.x}, {md.y})"),
        on_click=lambda md: print(f"点击: {md.button}"),
        on_scroll=lambda md: print(f"滚轮: delta={md.delta}"),
        on_drag=lambda md: print(f"拖拽: ({md.x}, {md.y})")
    )
    observer.attach(ms)

    print("监听鼠标... 按 Ctrl+C 退出")
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ms.stop()


def click_counter():
    """点击计数器"""
    ms = MouseSubject(backend="polling")
    ms.start()

    count = 0

    ms.pipe(
        ops.filter(lambda e: e.event_type == MouseEventType.LEFT_UP)
    ).subscribe(on_next=lambda e: (
        globals().update(count=globals().get('count', 0) + 1),
        print(f"左键点击次数: {globals()['count']}")
    ))

    print("点击计数... 按 Ctrl+C 退出")
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ms.stop()


def mouse_simulation():
    """鼠标模拟示例"""
    mouse = MouseDispatcher(backend="polling")
    mouse.start()

    # 移动鼠标
    mouse.move_to(500, 300)
    mouse.move_relative(10, 10)

    # 点击
    mouse.click("left")
    mouse.double_click("left")

    # 滚动
    mouse.scroll(120)   # 向上
    mouse.scroll(-120)  # 向下

    print("模拟操作完成")
    mouse.stop()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == "monitor":
            basic_mouse_monitor()
        elif mode == "observer":
            mouse_observer_example()
        elif mode == "count":
            click_counter()
        elif mode == "simulate":
            mouse_simulation()
        else:
            print(f"未知模式: {mode}")
    else:
        print("用法:")
        print("  python mouse_example.py monitor   - 基础鼠标监控")
        print("  python mouse_example.py observer   - 使用 MouseObserver")
        print("  python mouse_example.py count      - 点击计数")
        print("  python mouse_example.py simulate   - 鼠标模拟")