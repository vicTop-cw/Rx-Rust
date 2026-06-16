"""
test_file_watcher.py - rx-rust 文件系统监控模块综合测试

覆盖范围:
    1. 模块导入与 __all__ 完整性
    2. FileChangeType 枚举值 & str()
    3. FileData.now() 字段验证
    4. FileData 序列化往返（dict/json/pickle）
    5. FileDispatcher 生命周期（start/stop/with）
    6. FileDispatcher + polling 后端检测 CREATED/MODIFIED/DELETED
    7. change_types 白名单过滤
    8. FileSubject 上下文管理器 & pipe
    9. FileObserver 按类型路由
    10. from_filesystem 工厂
    11. write_to_filesystem 操作符
    12. ops.write_to_filesystem 集成
    13. 动态 add_path / remove_path
    14. 多次 start/stop 幂等性
    15. 线程安全测试（并发订阅/取消/stop）
"""

import os
import sys
import tempfile
import threading
import time

# 确保导入本地开发版本
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "rx-rust-py", "python"))

import rx_rust  # noqa: E402
from rx_rust import (  # noqa: E402
    FileChangeType,
    FileData,
    FileDispatcher,
    FileSubject,
    FileObserver,
    from_filesystem,
    write_to_filesystem,
    ops,
    Observable,
)


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def run_test(name, func):
    try:
        func()
        print(f"[PASS] {name}")
        return True
    except Exception as e:
        print(f"[FAIL] {name}: {e}")
        import traceback
        traceback.print_exc()
        return False


# -------------------------------------------------------------------
# Test 1: 模块导入与 __all__ 完整性
# -------------------------------------------------------------------
def test_1_import_and_all():
    expected = {
        "FileChangeType", "FileData", "FileDispatcher", "FileSubject",
        "FileObserver", "from_filesystem", "write_to_filesystem",
    }
    from rx_rust.file_watcher import __all__ as fw_all
    missing = expected - set(fw_all)
    _assert(not missing, f"__all__ 缺少: {missing}")

    # 检查主包中是否也导出了
    for name in expected:
        _assert(hasattr(rx_rust, name), f"rx_rust 缺少 {name}")


# -------------------------------------------------------------------
# Test 2: FileChangeType 枚举值 & str()
# -------------------------------------------------------------------
def test_2_change_type_values():
    expected = {
        0: "CREATED", 1: "MODIFIED", 2: "DELETED", 3: "RENAMED",
        4: "MOVED_IN", 5: "MOVED_OUT", 6: "ACCESS", 7: "ATTRIB",
    }
    for value, name in expected.items():
        ct = FileChangeType(value)
        _assert(int(ct) == value, f"{name} 的值应为 {value}，实为 {int(ct)}")
        _assert(str(ct) == name, f"{name} 的 str() 应为 '{name}'，实为 '{str(ct)}'")


# -------------------------------------------------------------------
# Test 3: FileData.now() 字段验证
# -------------------------------------------------------------------
def test_3_file_data_fields():
    fd = FileData.now(
        path="/tmp/test.txt",
        old_path="/tmp/old.txt",
        change_type=FileChangeType.RENAMED,
        is_directory=False,
        size=1024,
        tags=["test", "file"],
        metadata={"key": "value"},
    )
    _assert(fd.path == "/tmp/test.txt", f"path 错误: {fd.path}")
    _assert(fd.old_path == "/tmp/old.txt", f"old_path 错误: {fd.old_path}")
    _assert(fd.change_type == FileChangeType.RENAMED, "change_type 错误")
    _assert(fd.is_directory is False, "is_directory 错误")
    _assert(fd.size == 1024, f"size 错误: {fd.size}")
    _assert(fd.tags == ["test", "file"], f"tags 错误: {fd.tags}")
    _assert(fd.metadata == {"key": "value"}, f"metadata 错误: {fd.metadata}")
    _assert(fd.sequence > 0, "sequence 必须 > 0")
    # timestamp 应该是最近的时间
    now = time.time()
    ts = fd.timestamp.timestamp()
    _assert(abs(ts - now) < 5, f"timestamp 偏离当前时间过多: {ts} vs {now}")

    # sequence 单调递增
    fd2 = FileData.now(path="/tmp/test2.txt")
    _assert(fd2.sequence > fd.sequence, "sequence 应该单调递增")


# -------------------------------------------------------------------
# Test 4: FileData 序列化往返
# -------------------------------------------------------------------
def test_4_file_data_roundtrip():
    original = FileData.now(
        path="/tmp/rt.txt",
        old_path=None,
        change_type=FileChangeType.MODIFIED,
        is_directory=False,
        size=42,
        tags=["roundtrip", "test"],
        metadata={"a": 1, "b": "str"},
    )

    # dict 往返
    d = original.to_dict()
    restored = FileData.from_dict(d)
    _assert(restored.path == original.path, "dict 往返: path 不一致")
    _assert(restored.change_type == original.change_type, "dict 往返: change_type 不一致")
    _assert(restored.is_directory == original.is_directory, "dict 往返: is_directory 不一致")
    _assert(restored.size == original.size, "dict 往返: size 不一致")
    _assert(restored.tags == original.tags, "dict 往返: tags 不一致")
    _assert(restored.metadata == original.metadata, "dict 往返: metadata 不一致")

    # json 往返
    j = original.to_json()
    restored2 = FileData.from_json(j)
    _assert(restored2.path == original.path, "json 往返: path 不一致")
    _assert(restored2.change_type == original.change_type, "json 往返: change_type 不一致")
    _assert(restored2.size == original.size, "json 往返: size 不一致")

    # pickle 往返
    p = original.to_pickle()
    restored3 = FileData.from_pickle(p)
    _assert(restored3.path == original.path, "pickle 往返: path 不一致")
    _assert(restored3.change_type == original.change_type, "pickle 往返: change_type 不一致")


# -------------------------------------------------------------------
# Test 5: FileDispatcher 生命周期
# -------------------------------------------------------------------
def test_5_dispatcher_lifecycle():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = FileDispatcher(paths=[tmpdir], backend="polling", interval=0.1)
        _assert(not d.is_running, "初始状态应为非运行")
        d.start()
        _assert(d.is_running, "start 后应为运行")
        # 再次 start 应该幂等
        d.start()
        _assert(d.is_running, "第二次 start 不应出错")
        d.stop()
        _assert(not d.is_running, "stop 后应为非运行")
        # 再次 stop 应该幂等
        d.stop()


# -------------------------------------------------------------------
# Test 6: FileDispatcher 检测 CREATED/MODIFIED/DELETED
# -------------------------------------------------------------------
def test_6_detect_events():
    with tempfile.TemporaryDirectory() as tmpdir:
        events = []
        lock = threading.Lock()

        d = FileDispatcher(paths=[tmpdir], backend="polling", interval=0.05)
        d.subject.subscribe(on_next=lambda fd: events.append(fd))
        d.start()

        try:
            # 给监控一点时间
            time.sleep(0.1)

            # 创建文件
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, "w") as f:
                f.write("hello")

            # 等待事件被检测
            time.sleep(0.3)

            # 修改文件
            with open(test_file, "w") as f:
                f.write("world")
            time.sleep(0.3)

            # 删除文件
            os.remove(test_file)
            time.sleep(0.3)

            with lock:
                paths = [e.path for e in events]
                types = [e.change_type for e in events]

            _assert(
                any(e.change_type == FileChangeType.CREATED and test_file in e.path
                    for e in events),
                f"未检测到 CREATED 事件。收到事件: {[(e.path, str(e.change_type)) for e in events]}"
            )
            _assert(
                any(e.change_type == FileChangeType.MODIFIED and test_file in e.path
                    for e in events),
                f"未检测到 MODIFIED 事件。收到事件: {[(e.path, str(e.change_type)) for e in events]}"
            )
            _assert(
                any(e.change_type == FileChangeType.DELETED and test_file in e.path
                    for e in events),
                f"未检测到 DELETED 事件。收到事件: {[(e.path, str(e.change_type)) for e in events]}"
            )
        finally:
            d.stop()


# -------------------------------------------------------------------
# Test 7: change_types 白名单过滤
# -------------------------------------------------------------------
def test_7_change_types_filter():
    with tempfile.TemporaryDirectory() as tmpdir:
        received_types = []
        lock = threading.Lock()

        d = FileDispatcher(
            paths=[tmpdir],
            backend="polling",
            interval=0.05,
            change_types=[FileChangeType.CREATED],  # 只允许 CREATED
        )
        d.subject.subscribe(on_next=lambda fd: received_types.append(fd.change_type))
        d.start()

        try:
            time.sleep(0.1)
            test_file = os.path.join(tmpdir, "filter.txt")
            with open(test_file, "w") as f:
                f.write("content")
            time.sleep(0.3)
            with open(test_file, "w") as f:
                f.write("new content")
            time.sleep(0.3)
        finally:
            d.stop()

        with lock:
            _assert(
                FileChangeType.CREATED in received_types,
                f"应收到 CREATED，实为 {[str(t) for t in received_types]}"
            )
            _assert(
                FileChangeType.MODIFIED not in received_types,
                f"MODIFIED 应被过滤，但收到了。全部: {[str(t) for t in received_types]}"
            )


# -------------------------------------------------------------------
# Test 8: FileSubject 上下文管理器 & pipe
# -------------------------------------------------------------------
def test_8_file_subject():
    with tempfile.TemporaryDirectory() as tmpdir:
        received = []
        lock = threading.Lock()

        with FileSubject(paths=[tmpdir], backend="polling", interval=0.05) as fs:
            _assert(fs.is_running, "FileSubject 应该已启动")

            # 直接 subscribe
            fs.subscribe(on_next=lambda fd: received.append(fd))
            time.sleep(0.1)

            test_file = os.path.join(tmpdir, "subject.txt")
            with open(test_file, "w") as f:
                f.write("test")
            time.sleep(0.3)

            _assert(
                any("subject.txt" in fd.path for fd in received),
                f"FileSubject 订阅应收到事件，收到 {len(received)} 个"
            )
            _assert(fs.backend_name in ("polling", "win32", "inotify"),
                    f"backend_name: {fs.backend_name}")

        _assert(not fs.is_running, "退出 with 后应停止")


# -------------------------------------------------------------------
# Test 9: FileObserver 按类型路由
# -------------------------------------------------------------------
def test_9_file_observer():
    with tempfile.TemporaryDirectory() as tmpdir:
        created = []
        modified = []
        any_count = []

        obs = FileObserver(
            on_created=lambda fd: created.append(fd),
            on_modified=lambda fd: modified.append(fd),
            on_any=lambda fd: any_count.append(fd),
        )

        with FileSubject(paths=[tmpdir], backend="polling", interval=0.05) as fs:
            obs.subscribe(fs)
            time.sleep(0.1)

            test_file = os.path.join(tmpdir, "observer.txt")
            with open(test_file, "w") as f:
                f.write("initial")
            time.sleep(0.3)

            with open(test_file, "w") as f:
                f.write("updated")
            time.sleep(0.3)

        _assert(
            any("observer.txt" in fd.path for fd in created),
            f"on_created 未被调用，created={len(created)}"
        )
        _assert(
            any("observer.txt" in fd.path for fd in modified),
            f"on_modified 未被调用，modified={len(modified)}"
        )
        _assert(len(any_count) >= 2, f"on_any 应该收到至少 2 个事件，实为 {len(any_count)}")

        # 测试 unsubscribe
        _assert(obs.is_subscribed, "subscribe 后 is_subscribed 应为 True")
        obs.unsubscribe()
        _assert(not obs.is_subscribed, "unsubscribe 后 is_subscribed 应为 False")


# -------------------------------------------------------------------
# Test 10: from_filesystem 工厂
# -------------------------------------------------------------------
def test_10_from_filesystem():
    with tempfile.TemporaryDirectory() as tmpdir:
        received = []
        obs, d = from_filesystem(paths=[tmpdir], backend="polling", interval=0.05)
        try:
            _assert(d.is_running, "auto_start=True 时应已启动")
            obs.subscribe(on_next=lambda fd: received.append(fd))

            time.sleep(0.1)
            test_file = os.path.join(tmpdir, "factory.txt")
            with open(test_file, "w") as f:
                f.write("test")
            time.sleep(0.3)

            _assert(
                any("factory.txt" in fd.path for fd in received),
                f"from_filesystem 应收到事件，收到 {len(received)} 个"
            )
        finally:
            d.stop()

    # 测试 auto_start=False
    with tempfile.TemporaryDirectory() as tmpdir:
        _, d2 = from_filesystem(
            paths=[tmpdir], backend="polling", interval=0.05, auto_start=False
        )
        _assert(not d2.is_running, "auto_start=False 时不应启动")


# -------------------------------------------------------------------
# Test 11: write_to_filesystem 操作符
# -------------------------------------------------------------------
def test_11_write_to_filesystem():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = FileDispatcher(paths=[tmpdir], backend="polling", interval=0.1)

        try:
            # 测试 tuple/list 作为上游输入
            test_path = os.path.join(tmpdir, "written.txt")
            content = "hello from operator"

            source = Observable.from_iter([(test_path, content)])
            results = []
            result_obs = ops.write_to_filesystem(d, mode="create")(source)
            result_obs.subscribe(on_next=lambda fd: results.append(fd))
            time.sleep(0.2)

            _assert(
                os.path.exists(test_path),
                f"写入的文件不存在: {test_path}"
            )
            with open(test_path, "r", encoding="utf-8") as f:
                actual = f.read()
            _assert(actual == content, f"文件内容错误: '{actual}' vs '{content}'")
            _assert(len(results) >= 1, f"应收到写入后 FileData，实为 {len(results)}")
            if results:
                _assert(
                    results[0].change_type == FileChangeType.CREATED,
                    f"change_type 应为 CREATED，实为 {results[0].change_type}"
                )
                _assert(
                    test_path in results[0].path,
                    f"FileData.path 错误: {results[0].path}"
                )

            # 测试 dict 作为上游
            dict_path = os.path.join(tmpdir, "dict_written.txt")
            dict_content = "dict content"
            results2 = []
            source2 = Observable.from_iter([{
                "path": dict_path, "content": dict_content,
                "change_type": FileChangeType.MODIFIED, "tags": ["tag1"],
                "metadata": {"k": "v"},
            }])
            ops.write_to_filesystem(d, mode="create")(source2).subscribe(
                on_next=lambda fd: results2.append(fd)
            )
            time.sleep(0.2)
            _assert(os.path.exists(dict_path), "dict 上游写入失败")
        finally:
            d.stop()


# -------------------------------------------------------------------
# Test 12: ops.write_to_filesystem 集成
# -------------------------------------------------------------------
def test_12_ops_integration():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = FileDispatcher(paths=[tmpdir], backend="polling", interval=0.1)

        try:
            test_path = os.path.join(tmpdir, "ops_test.txt")
            content = "ops integration"

            results = []
            src = Observable.from_iter([(test_path, content)])
            ops.write_to_filesystem(d, mode="create")(src).subscribe(
                on_next=lambda fd: results.append(fd)
            )
            time.sleep(0.3)

            _assert(os.path.exists(test_path), "ops.write_to_filesystem 应写入文件")
            _assert(len(results) >= 1, "ops 集成应收到 FileData")
        finally:
            d.stop()


# -------------------------------------------------------------------
# Test 13: 动态 add_path / remove_path
# -------------------------------------------------------------------
def test_13_add_remove_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        path_a = os.path.join(tmpdir, "a")
        path_b = os.path.join(tmpdir, "b")
        os.makedirs(path_a)
        os.makedirs(path_b)

        received_a = []
        received_b = []
        lock = threading.Lock()

        # 初始只监控 path_a
        d = FileDispatcher(paths=[path_a], backend="polling", interval=0.05)
        d.subject.subscribe(on_next=lambda fd: received_a.append(fd))
        d.start()

        try:
            time.sleep(0.1)

            # path_a 中创建文件
            f_a = os.path.join(path_a, "file_a.txt")
            with open(f_a, "w") as f:
                f.write("in a")
            time.sleep(0.3)
            _assert(
                any("file_a.txt" in fd.path for fd in received_a),
                f"应收到 path_a 中的事件，收到 {len(received_a)} 个"
            )

            # 监控 path_b 中的第一个事件
            stage2_events = []
            d.subject.subscribe(on_next=lambda fd: stage2_events.append(fd))
            d.add_path(path_b)
            time.sleep(0.2)

            f_b = os.path.join(path_b, "file_b.txt")
            with open(f_b, "w") as f:
                f.write("in b")
            time.sleep(0.3)

            _assert(
                any("file_b.txt" in fd.path for fd in stage2_events),
                f"add_path 后应收到 path_b 的事件，stage2={[fd.path for fd in stage2_events]}"
            )

            # 移除 path_b
            d.remove_path(path_b)
            time.sleep(0.2)

            stage3_events = []
            d.subject.subscribe(on_next=lambda fd: stage3_events.append(fd))
            f_b2 = os.path.join(path_b, "file_b2.txt")
            with open(f_b2, "w") as f:
                f.write("should not detect")
            time.sleep(0.3)

            # 不应收到 path_b 中新文件的事件
            _assert(
                not any("file_b2.txt" in fd.path for fd in stage3_events),
                f"remove_path 后不应收到 path_b 中的事件，stage3={[fd.path for fd in stage3_events]}"
            )
        finally:
            d.stop()


# -------------------------------------------------------------------
# Test 14: 多次 start/stop 幂等性
# -------------------------------------------------------------------
def test_14_start_stop_idempotent():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = FileDispatcher(paths=[tmpdir], backend="polling", interval=0.1)
        # 连续多次 start/stop
        for _ in range(5):
            d.start()
        _assert(d.is_running, "多次 start 后应保持运行")

        for _ in range(5):
            d.stop()
        _assert(not d.is_running, "多次 stop 后应停止")

        # 重启后再测试事件
        d.start()
        received = []
        d.subject.subscribe(on_next=lambda fd: received.append(fd))
        time.sleep(0.1)
        test_file = os.path.join(tmpdir, "idempotent.txt")
        with open(test_file, "w") as f:
            f.write("x")
        time.sleep(0.3)
        d.stop()
        _assert(
            any("idempotent.txt" in fd.path for fd in received),
            f"重启后应能检测事件，收到 {len(received)} 个"
        )


# -------------------------------------------------------------------
# Test 15: 线程安全测试（并发订阅/取消/stop）
# -------------------------------------------------------------------
def test_15_thread_safety():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = FileDispatcher(paths=[tmpdir], backend="polling", interval=0.05)
        d.start()

        errors = []

        def worker(i):
            try:
                # 并发订阅
                subs = []
                for j in range(3):
                    s = d.subject.subscribe(on_next=lambda fd, w=i: None)
                    subs.append(s)
                time.sleep(0.05)
                # 并发创建文件
                f = os.path.join(tmpdir, f"thread_{i}.txt")
                with open(f, "w") as fh:
                    fh.write(str(i))
            except Exception as e:
                errors.append((i, e))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # stop 期间也可能有事件
        d.stop()
        d.stop()  # 再次 stop 也不应崩溃

        _assert(len(errors) == 0, f"并发错误: {errors}")


# -------------------------------------------------------------------
# 主运行
# -------------------------------------------------------------------
def main():
    tests = [
        ("1. 模块导入与 __all__", test_1_import_and_all),
        ("2. FileChangeType 枚举值", test_2_change_type_values),
        ("3. FileData.now() 字段验证", test_3_file_data_fields),
        ("4. FileData 序列化往返", test_4_file_data_roundtrip),
        ("5. FileDispatcher 生命周期", test_5_dispatcher_lifecycle),
        ("6. 检测 CREATED/MODIFIED/DELETED", test_6_detect_events),
        ("7. change_types 白名单过滤", test_7_change_types_filter),
        ("8. FileSubject 上下文管理器 & pipe", test_8_file_subject),
        ("9. FileObserver 按类型路由", test_9_file_observer),
        ("10. from_filesystem 工厂", test_10_from_filesystem),
        ("11. write_to_filesystem 操作符", test_11_write_to_filesystem),
        ("12. ops.write_to_filesystem 集成", test_12_ops_integration),
        ("13. 动态 add_path / remove_path", test_13_add_remove_path),
        ("14. 多次 start/stop 幂等性", test_14_start_stop_idempotent),
        ("15. 线程安全测试", test_15_thread_safety),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        if run_test(name, fn):
            passed += 1
        else:
            failed += 1

    print()
    print(f"=" * 60)
    print(f"总结: {passed} 通过 / {failed} 失败 / {len(tests)} 总计")
    print(f"=" * 60)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
