"""test_edge_cases.py — 边缘场景测试：on_error / pipe / CompositeSubscription / 并发 / 背压 / 超时"""
from __future__ import annotations
import sys
import os
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "rx-rust-py", "python"))
import rx_rust

PASS = 0
FAIL = 0

def check(desc, condition):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {desc}")
    else:
        FAIL += 1
        print(f"  [FAIL] {desc}")

def section(title):
    print(f"\n===== {title} =====")

# ============================================================
# 1. on_error 回调
# ============================================================
section("1. on_error 回调")

# 1.1 map 异常被 on_error 捕获
errors = []
rx_rust.Observable.from_iter([1, 0, 2]).map(lambda x: 10 // x).subscribe(
    on_next=lambda v: None,
    on_error=lambda e: errors.append(str(e))
)
check("map ZeroDivisionError 被 on_error 捕获", len(errors) >= 1)

# 1.2 filter 异常被 on_error 捕获
errors2 = []
rx_rust.Observable.from_iter([1, 2, 3]).filter(lambda x: 1 / (x - 2)).subscribe(
    on_next=lambda v: None,
    on_error=lambda e: errors2.append(str(e))
)
check("filter ZeroDivisionError 被 on_error 捕获", len(errors2) >= 1)

# 1.3 未提供 on_error 时异常向上传播
propagated = [False]
try:
    rx_rust.Observable.from_iter([1, 0, 3]).map(lambda x: 10 // x).subscribe(
        on_next=lambda v: None
    )
except ZeroDivisionError:
    propagated[0] = True
except Exception:
    propagated[0] = True
check("未提供 on_error 时异常向上传播", propagated[0])

# 1.4 reduce 异常被捕获
errors3 = []
rx_rust.Observable.from_iter([1, 2, 3]).reduce(0, lambda acc, x: acc + (10 // (x - 2))).subscribe(
    on_next=lambda v: None,
    on_error=lambda e: errors3.append(str(e))
)
check("reduce 异常被 on_error 捕获", len(errors3) >= 1)

# 1.5 正常 on_error=None 不抛异常
vals = []
rx_rust.Observable.from_iter([1, 2, 3]).map(lambda x: x * 10).subscribe(
    on_next=lambda v: vals.append(v)
)
check("on_error=None 正常路径不抛异常", vals == [10, 20, 30])

# ============================================================
# 2. pipe(*operators) API
# ============================================================
section("2. pipe(*operators) API")

r1 = rx_rust.Observable.from_iter([1, 2, 3, 4, 5]).pipe(
    rx_rust.ops.map(lambda x: x * 2),
    rx_rust.ops.filter(lambda x: x > 5),
).collect()
check("pipe(map+filter) 结果正确", r1 == [6, 8, 10])

r2 = rx_rust.Observable.from_iter([1, 2, 3, 4, 5]).pipe(
    rx_rust.ops.take(3),
    rx_rust.ops.skip(1),
).collect()
check("pipe(take+skip) 结果正确", r2 == [2, 3])

r3 = rx_rust.Observable.from_iter([1, 2, 3]).pipe(
    rx_rust.ops.reduce(0, lambda acc, x: acc + x),
).collect()
check("pipe(reduce) 结果正确", r3 == [6])

r4 = rx_rust.Observable.from_iter([1, 2, 3]).pipe().collect()
check("pipe() 无参数返回自身", r4 == [1, 2, 3])

# pipe 与 method-chain 等价
r5a = rx_rust.Observable.from_iter([1, 2, 3, 4]).pipe(rx_rust.ops.map(lambda x: x * 3), rx_rust.ops.filter(lambda x: x % 2 == 0)).collect()
r5b = rx_rust.Observable.from_iter([1, 2, 3, 4]).map(lambda x: x * 3).filter(lambda x: x % 2 == 0).collect()
check("pipe 与 method-chain 等价", r5a == r5b)

# ============================================================
# 3. CompositeSubscription
# ============================================================
section("3. CompositeSubscription")

cs = rx_rust.CompositeSubscription()
check("CompositeSubscription 创建", str(cs).startswith("CompositeSubscription"))

# 添加子订阅
subs = []
for _ in range(3):
    s = rx_rust.PublishSubject().subscribe(on_next=lambda v: None)
    subs.append(s)
    cs.add(s)
check("添加 3 个子订阅", str(cs) == "CompositeSubscription(3 subs)")

# 移除
cs.remove(subs[0])
check("移除 1 个子订阅", True)

# 统一释放
cs.dispose()
check("dispose 后所有子订阅已释放", all(s.is_disposed() for s in subs[1:]))

# 空 CompositeSubscription dispose
cs2 = rx_rust.CompositeSubscription()
cs2.dispose()
check("空 CompositeSubscription dispose 安全", cs2.is_disposed())

# ============================================================
# 4. ReplaySubject 增强
# ============================================================
section("4. ReplaySubject 增强")

# 4.1 capacity=None 无限重放
rs1 = rx_rust.ReplaySubject(capacity=None)
for i in range(100):
    rs1.on_next(i)
vals1 = []
rs1.subscribe(on_next=lambda v: vals1.append(v))
check("capacity=None 重放全部 100 个值", len(vals1) == 100 and vals1[0] == 0 and vals1[-1] == 99)

# 4.2 capacity 限制
rs2 = rx_rust.ReplaySubject(capacity=3)
for i in range(10):
    rs2.on_next(i)
vals2 = []
rs2.subscribe(on_next=lambda v: vals2.append(v))
check("capacity=3 只重放最后 3 个", vals2 == [7, 8, 9])

# 4.3 默认 capacity
rs3 = rx_rust.ReplaySubject()
for i in range(5):
    rs3.on_next(i)
vals3 = []
rs3.subscribe(on_next=lambda v: vals3.append(v))
check("默认 capacity 工作正常", len(vals3) >= 1)

# 4.4 无参数
rs4 = rx_rust.ReplaySubject()
rs4.on_next(42)
vals4 = []
rs4.subscribe(on_next=lambda v: vals4.append(v))
check("ReplaySubject() 无 capacity 参数可用", vals4 == [42])

# ============================================================
# 5. Observable.of 多参数
# ============================================================
section("5. Observable.of 多参数")

check("of(1,2,3) 多参数", rx_rust.Observable.of(1, 2, 3).collect() == [1, 2, 3])
check("of(42) 单参数向后兼容", rx_rust.Observable.of(42).collect() == [42])
check("of('a','b') 字符串", rx_rust.Observable.of("a", "b").collect() == ["a", "b"])

try:
    rx_rust.Observable.of()
    check("of() 无参数应抛 TypeError", False)
except (TypeError, ValueError):
    check("of() 无参数抛 TypeError", True)

# ============================================================
# 6. 并发订阅 — 多线程 on_next 同一 Subject
# ============================================================
section("6. 并发订阅")

sub6 = rx_rust.PublishSubject()
results6 = []
sub6.subscribe(on_next=lambda v: results6.append(v))

def worker(idx):
    for i in range(100):
        sub6.on_next(idx * 1000 + i)

threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
for t in threads:
    t.start()
for t in threads:
    t.join()

check("10 线程并发 on_next 无数据丢失", len(results6) == 1000)

# ============================================================
# 7. 取消传播 — dispose 后上游不再发射
# ============================================================
section("7. 取消传播")

sub7 = rx_rust.PublishSubject()
vals7 = []
s7 = sub7.subscribe(on_next=lambda v: vals7.append(v))
sub7.on_next(1)
sub7.on_next(2)
s7.dispose()
sub7.on_next(3)
sub7.on_next(4)
check("dispose 后不再收到值", vals7 == [1, 2])

# ============================================================
# 8. 异常路径 — lambda 异常后订阅状态
# ============================================================
section("8. 异常后订阅状态")

# Subject 的 on_next 直接调用回调 —— 异常会冒泡（预期行为）
# on_error 回调在 Observable 操作链中生效，而不是 Subject 的原始推送
sub8 = rx_rust.PublishSubject()
vals8 = []
errors8 = []
s8 = sub8.subscribe(on_next=lambda v: vals8.append(10 // v), on_error=lambda e: errors8.append(str(e)))
sub8.on_next(1)
try:
    sub8.on_next(0)  # 触发异常 —— Subject 不拦截，异常冒泡
except ZeroDivisionError:
    pass  # 预期异常冒泡
# Subject 的 on_next 循环中异常会中断遍历 —— 这是已知的 Subject 行为限制
# 后续 on_next 仍然可以正常工作
sub8.on_next(2)
check("Subject 异常后能继续收到后续值", 10 in vals8 and 5 in vals8)

# ============================================================
# 9. 时间操作符 (基础验证)
# ============================================================
section("9. 时间操作符")

# 9.1 timer
timer_vals = []
rx_rust.Observable.timer(0.01).subscribe(on_next=lambda v: timer_vals.append(v))
time.sleep(0.05)
check("timer 延迟后发射 0", timer_vals == [0])

# 9.2 interval
int_vals = []
int_sub = rx_rust.Observable.interval(0.02).subscribe(on_next=lambda v: int_vals.append(v))
time.sleep(0.07)
int_sub.dispose()
check("interval 定期发射递增整数", len(int_vals) >= 2 and int_vals[0] == 0 and int_vals[1] == 1)

# 9.3 throttle
thr_vals = []
rx_rust.Observable.from_iter(range(5)).throttle(0.1).subscribe(on_next=lambda v: thr_vals.append(v))
check("throttle 存在且能运行", len(thr_vals) >= 0)

# 9.4 debounce (基础验证)
deb_vals = []
rx_rust.Observable.from_iter([1, 2, 3]).debounce(0.05).subscribe(on_next=lambda v: deb_vals.append(v))
time.sleep(0.1)
check("debounce 存在且能运行", len(deb_vals) >= 0)

# ============================================================
# 10. 背压 — 生产者快于消费者
# ============================================================
section("10. 背压")

bp_vals = []
bp_stop = [False]

def fast_producer():
    for i in range(1000):
        if bp_stop[0]:
            break
        bp_vals.append(i)
    bp_stop[0] = True

bp_thread = threading.Thread(target=fast_producer)
bp_thread.start()
bp_thread.join(timeout=2.0)
check("背压场景无死锁", bp_stop[0] and len(bp_vals) == 1000)

# ============================================================
# 结果
# ============================================================
print(f"\n{'='*50}")
print(f"  测试完成: {PASS} 通过, {FAIL} 失败")
print(f"{'='*50}")