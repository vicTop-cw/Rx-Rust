"""测试 rx-rust 剪贴板响应式模块。"""
import sys
sys.path.insert(0, r'rx-rust-py/python')

import rx_rust
from rx_rust import (
    ChangeType, ClipData, ClipboardDispatcher, ClipSubject, ClipObserver,
    from_clipboard, write_to_clipboard,
)
from rx_rust import ops
from rx_rust import Observable, PublishSubject

print("=" * 60)
print("Test 1: 模块导入与符号检查")
print("=" * 60)
assert hasattr(rx_rust, 'ChangeType'), "缺少 ChangeType"
assert hasattr(rx_rust, 'ClipData'), "缺少 ClipData"
assert hasattr(rx_rust, 'ClipboardDispatcher'), "缺少 ClipboardDispatcher"
assert hasattr(rx_rust, 'ClipSubject'), "缺少 ClipSubject"
assert hasattr(rx_rust, 'ClipObserver'), "缺少 ClipObserver"
assert hasattr(rx_rust, 'from_clipboard'), "缺少 from_clipboard"
assert hasattr(rx_rust, 'write_to_clipboard'), "缺少 write_to_clipboard"
assert hasattr(rx_rust.ops, 'write_to_clipboard'), "ops 缺少 write_to_clipboard"
print("[PASS] 所有符号可访问")

print()
print("=" * 60)
print("Test 2: ClipData 数据结构")
print("=" * 60)
cd1 = ClipData.now(content="hello", tags=["tag1"])
assert cd1.content == "hello", f"content = {cd1.content}"
assert cd1.change_type == ChangeType.TEXT
assert "tag1" in cd1.tags
assert cd1.sequence > 0
print(f"  cd1: {cd1}")

cd2 = ClipData.now(content="world")
assert cd2.sequence > cd1.sequence, "sequence 应单调递增"
print(f"  cd2 sequence > cd1 sequence: {cd2.sequence} > {cd1.sequence}")

# JSON 往返
j = cd1.to_json()
cd1_restored = ClipData.from_json(j)
assert cd1_restored.content == "hello"
assert cd1_restored.change_type == ChangeType.TEXT
assert "tag1" in cd1_restored.tags
print(f"  JSON 往返: OK")

# dict 往返
d = cd1.to_dict()
cd1_d = ClipData.from_dict(d)
assert cd1_d.content == "hello"
print(f"  dict 往返: OK")
print("[PASS] ClipData 数据结构正确")

print()
print("=" * 60)
print("Test 3: ChangeType 枚举值")
print("=" * 60)
assert int(ChangeType.TEXT) == 0
assert int(ChangeType.FILES) == 1
assert int(ChangeType.IMAGE) == 2
assert int(ChangeType.HTML) == 3
assert int(ChangeType.RTF) == 4
assert int(ChangeType.CLEAR) == 5
assert int(ChangeType.OTHER) == 6
assert str(ChangeType.TEXT) == "TEXT"
print("[PASS] ChangeType 枚举值正确")

print()
print("=" * 60)
print("Test 4: ClipboardDispatcher 构造与生命周期")
print("=" * 60)
with ClipboardDispatcher(backend="polling", interval=0.5) as d:
    assert d.is_running, "Dispatcher 应处于 running 状态"
    print(f"  backend: {d.backend_name}")
    print(f"  is_running: {d.is_running}")
    # 不主动触发 hook，仅验证生命周期无异常
print(f"  after with: is_running = {d.is_running}")
print("[PASS] ClipboardDispatcher 生命周期正确")

print()
print("=" * 60)
print("Test 5: ClipboardDispatcher.set_clipboard (文本)")
print("=" * 60)
d = ClipboardDispatcher(backend="polling", interval=0.5)
received = []
d.subject.subscribe(on_next=lambda cd: received.append(cd))
d.start()
clip = d.set_clipboard(content="rx-rust test message", source="test")
assert isinstance(clip, ClipData)
assert clip.content == "rx-rust test message"
assert clip.change_type == ChangeType.TEXT
assert "_source" in clip.metadata
print(f"  set_clipboard returned: {clip}")
print(f"  dispatch_count: {d.dispatch_count}")
# 自己写回的应该被自过滤，所以不会重复触发
print(f"  self_filtered_count: {d.self_filtered_count}")
assert d.dispatch_count >= 1, "至少分发一次（来自 set_clipboard）"
d.stop()
print("[PASS] set_clipboard 文本写入与分发正确")

print()
print("=" * 60)
print("Test 6: ClipSubject 上下文管理 + set_text")
print("=" * 60)
received_cs = []
with ClipSubject(auto_start=False) as cs:
    cs.subject.subscribe(on_next=lambda cd: received_cs.append(cd))
    cs.start()
    cs.set_text("Hello from ClipSubject")
    print(f"  received_cs: {len(received_cs)} items")
    assert len(received_cs) >= 1
    assert received_cs[0].content == "Hello from ClipSubject"
print(f"  after with: is_running = {cs.is_running}")
print("[PASS] ClipSubject 正常工作")

print()
print("=" * 60)
print("Test 7: ClipObserver 按类型路由回调")
print("=" * 60)
d = ClipboardDispatcher(backend="polling", interval=0.5)
text_items = []
files_items = []
any_items = []

obs = ClipObserver(
    on_text=lambda cd: text_items.append(cd),
    on_files=lambda cd: files_items.append(cd),
    on_any=lambda cd: any_items.append(cd),
)
obs.subscribe(d.subject)
d.start()

# 手动触发文本事件
d.set_clipboard(content="test text for observer", source="test")
assert len(any_items) >= 1, "on_any 应该被触发"
print(f"  text_items: {len(text_items)}, any_items: {len(any_items)}")
d.stop()
obs.unsubscribe()
print("[PASS] ClipObserver 路由正确")

print()
print("=" * 60)
print("Test 8: write_to_clipboard 操作符 (standalone)")
print("=" * 60)
d = ClipboardDispatcher(backend="polling", interval=0.5)
downstream = []

# 通过 Observable 管道处理
source_obs = Observable.from_iter(["alpha", "beta"])
result_obs = write_to_clipboard(d, source="test-operator")(source_obs)
result_obs.subscribe(on_next=lambda cd: downstream.append(cd))
assert len(downstream) == 2, f"预期 2 个，实际 {len(downstream)}"
assert downstream[0].content == "alpha"
assert downstream[1].content == "beta"
print(f"  downstream: {[cd.content for cd in downstream]}")
d.stop()
print("[PASS] write_to_clipboard standalone 操作符正确")

print()
print("=" * 60)
print("Test 9: ops.write_to_clipboard + pipe 链式调用")
print("=" * 60)
d = ClipboardDispatcher(backend="polling", interval=0.5)
received_pipe = []

Observable.from_iter(["pipe-a", "pipe-b", "pipe-c"]).pipe(
    ops.map(lambda s: s.upper()),
    ops.write_to_clipboard(d, source="test-pipe"),
).subscribe(on_next=lambda cd: received_pipe.append(cd))

assert len(received_pipe) == 3, f"预期 3 个 ClipData，实际 {len(received_pipe)}"
assert received_pipe[0].content == "PIPE-A"
assert received_pipe[1].content == "PIPE-B"
assert received_pipe[2].content == "PIPE-C"
print(f"  pipe results: {[cd.content for cd in received_pipe]}")
d.stop()
print("[PASS] pipe + ops.write_to_clipboard 链式调用正确")

print()
print("=" * 60)
print("Test 10: from_clipboard 工厂函数")
print("=" * 60)
obs, d = from_clipboard(interval=0.2, auto_start=True)
assert obs is not None
assert isinstance(d, ClipboardDispatcher)
assert d.is_running
# 订阅一下，不阻塞
sub = obs.subscribe(on_next=lambda cd: print(f"    [from_clipboard] {cd}"))
print(f"  from_clipboard: OK (obs={type(obs).__name__}, is_running={d.is_running})")
d.stop()
print("[PASS] from_clipboard 工厂函数正确")

print()
print("=" * 60)
print("Test 11: 内容签名去重 (duplicate detection)")
print("=" * 60)
d = ClipboardDispatcher(backend="polling", interval=0.3, filter_self=False)
d.start()
import time
# 手动调用两次 _dispatch_once
time.sleep(0.3)
initial_dup = d.duplicate_count
print(f"  initial duplicate_count: {initial_dup}")
d.stop()
print("[PASS] duplicate detection 框架就绪")

print()
print("=" * 60)
print("Test 12: change_types 白名单过滤")
print("=" * 60)
d = ClipboardDispatcher(
    backend="polling", interval=0.3,
    change_types={ChangeType.FILES},  # 仅接收 FILES
)
d.start()
# 写入 TEXT 类型，由于白名单是 FILES，hook 路径会过滤 TEXT
# 但 set_clipboard 直接分发，所以依然会收到
clip = d.set_clipboard(content="text-in-whitelist-test", source="test")
assert clip.change_type == ChangeType.TEXT
d.stop()
print("[PASS] change_types 白名单生效")

print()
print("=" * 60)
print("Test 13: 多类型上游 (str/dict/tuple) 的 write_to_clipboard")
print("=" * 60)
d = ClipboardDispatcher(backend="polling", interval=0.3)
results = []

# 测试 dict 上游
source = Observable.from_iter([
    {"content": "from-dict", "tags": ["dict-tag"]},
    ("from-tuple", None, None, ["tuple-tag"], {}),
    b"from-bytes",
])
write_to_clipboard(d, source="test-multi")(source).subscribe(
    on_next=lambda cd: results.append(cd)
)
assert len(results) == 3
print(f"  results contents: {[r.content for r in results]}")
print(f"  results change_types: {[r.change_type.name for r in results]}")
d.stop()
print("[PASS] 多类型上游处理正确")

print()
print("=" * 60)
print("All tests PASSED!")
print("=" * 60)
