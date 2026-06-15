"""
Rx-Rust 基础示例

展示 Observable、Observer、Subject 的基本用法。
"""

from rx_rust import Observable, Observer, PublishSubject


def basic_observable():
    """基础 Observable 示例"""
    # 创建一个简单的 Observable
    obs = Observable.create(lambda observer: [
        observer.on_next(1),
        observer.on_next(2),
        observer.on_next(3),
        observer.on_completed()
    ])

    # 使用 Observer 订阅
    observer = Observer(
        on_next=lambda x: print(f"收到: {x}"),
        on_error=lambda e: print(f"错误: {e}"),
        on_completed=lambda: print("完成!")
    )

    obs.subscribe(observer)


def basic_subject():
    """基础 Subject 示例"""
    subject = PublishSubject()

    # 多个订阅者
    subject.subscribe(on_next=lambda x: print(f"订阅者1: {x}"))
    subject.subscribe(on_next=lambda x: print(f"订阅者2: {x}"))

    # 发送数据
    subject.on_next("Hello")
    subject.on_next("World")
    subject.on_completed()


def basic_operators():
    """基础操作符示例"""
    from rx_rust import ops

    obs = Observable.create(lambda o: [
        o.on_next(1),
        o.on_next(2),
        o.on_next(3),
        o.on_next(4),
        o.on_next(5),
        o.on_completed()
    ])

    # 使用操作符链
    obs.pipe(
        ops.filter(lambda x: x > 2),
        ops.map(lambda x: x * 10),
        ops.take(2)
    ).subscribe(on_next=lambda x: print(f"结果: {x}"))


if __name__ == "__main__":
    print("=== 基础 Observable ===")
    basic_observable()

    print("\n=== 基础 Subject ===")
    basic_subject()

    print("\n=== 基础操作符 ===")
    basic_operators()