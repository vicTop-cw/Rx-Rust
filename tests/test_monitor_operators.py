"""测试 Rx-Rust 监控专用操作符"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "rx-rust-py", "python"))

import rx_rust
from rx_rust import Observable, ops

print("=== 测试监控专用操作符 ===\n")

# 测试 filter_by_event_type (Rust实现)
print("1. 测试 filter_by_event_type (Rust):")
class MockEvent:
    def __init__(self, event_type, data):
        self.event_type = event_type
        self.data = data

events = [
    MockEvent('KEY_DOWN', 'A'),
    MockEvent('KEY_UP', 'A'),
    MockEvent('KEY_DOWN', 'B'),
    MockEvent('KEY_HOLD', 'B'),
]

result1 = []
Observable.from_iter(events).pipe(
    ops.filter_by_event_type('KEY_DOWN')
).subscribe(on_next=lambda e: result1.append(e.data))
print(f"   过滤 KEY_DOWN 事件: {result1}")

# 测试 throttle (Rust实现 - 直接调用Observable方法)
print("\n2. 测试 throttle (Rust):")
import time

result2 = []
obs = Observable.interval(0.1)
subscription = obs.throttle(0.3).subscribe(on_next=lambda v: result2.append(v))
time.sleep(1.0)
subscription.dispose()
time.sleep(0.1)
print(f"   节流后事件数量: {len(result2)} (预期: ~3-4)")

# 测试 filter (Rust实现)
print("\n3. 测试 filter (Rust):")
result3 = []
Observable.of(1, 2, 3, 4, 5).pipe(
    ops.filter(lambda x: x % 2 == 0)
).subscribe(on_next=lambda v: result3.append(v))
print(f"   过滤偶数: {result3}")

# 测试 map (Rust实现)
print("\n4. 测试 map (Rust):")
result4 = []
Observable.of(1, 2, 3).pipe(
    ops.map(lambda x: x * 2)
).subscribe(on_next=lambda v: result4.append(v))
print(f"   乘以2: {result4}")

# 测试 reduce (Rust实现)
print("\n5. 测试 reduce (Rust):")
result5 = []
Observable.of(1, 2, 3, 4, 5).pipe(
    ops.reduce(0, lambda acc, x: acc + x)
).subscribe(on_next=lambda v: result5.append(v))
print(f"   求和: {result5}")

print("\n=== 所有监控操作符测试完成 ===")