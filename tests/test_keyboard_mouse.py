"""
test_keyboard_mouse.py - 键鼠操作模块集成测试

测试覆盖：
    1. 类型导入与枚举值验证
    2. KeyData / MouseData 构造与序列化
    3. KeyboardDispatcher / MouseDispatcher 生命周期
    4. 订阅与回调
    5. 模拟操作（不依赖真实 GUI）
    6. 顶层工厂函数
    7. Subject / Observer
    8. 写入操作符

运行方式：
    pytest test_keyboard_mouse.py -v
    或
    python test_keyboard_mouse.py
"""

import sys
import time
import unittest

# 优先使用 Rust 扩展
USE_RUST = False
try:
    from rx_rust import (
        KeyEventType,
        KeyData,
        MouseEventType,
        MouseData,
        KeyModifier,
        KeyboardDispatcher,
        MouseDispatcher,
        KeySubject,
        MouseSubject,
        KeyObserver,
        MouseObserver,
        from_keyboard,
        from_mouse,
        write_to_keyboard,
        write_to_mouse,
    )
    USE_RUST = True
    print("[TEST] 使用 Rust 扩展")
except ImportError as e:
    print(f"[TEST] 无法导入 Rust 扩展: {e}")
    print("[TEST] 将测试 Python 类型别名（如果存在）")


class TestKeyEventType(unittest.TestCase):
    """测试键盘事件类型枚举"""

    def test_enum_values(self):
        """KeyEventType 常量值正确"""
        self.assertEqual(int(KeyEventType.KEY_DOWN), 0)
        self.assertEqual(int(KeyEventType.KEY_UP), 1)
        self.assertEqual(int(KeyEventType.KEY_HOLD), 2)

    def test_enum_str(self):
        """KeyEventType 字符串表示正确"""
        self.assertEqual(str(KeyEventType.KEY_DOWN), "KEY_DOWN")
        self.assertEqual(str(KeyEventType.KEY_UP), "KEY_UP")
        self.assertEqual(str(KeyEventType.KEY_HOLD), "KEY_HOLD")

    def test_enum_repr(self):
        """KeyEventType repr 表示正确"""
        self.assertIn("KeyEventType", repr(KeyEventType.KEY_DOWN))
        self.assertIn("KEY_DOWN", repr(KeyEventType.KEY_DOWN))


class TestMouseEventType(unittest.TestCase):
    """测试鼠标事件类型枚举"""

    def test_enum_values(self):
        """MouseEventType 常量值正确"""
        self.assertEqual(int(MouseEventType.MOVE), 0)
        self.assertEqual(int(MouseEventType.LEFT_DOWN), 1)
        self.assertEqual(int(MouseEventType.LEFT_UP), 2)
        self.assertEqual(int(MouseEventType.RIGHT_DOWN), 3)
        self.assertEqual(int(MouseEventType.RIGHT_UP), 4)
        self.assertEqual(int(MouseEventType.MIDDLE_DOWN), 5)
        self.assertEqual(int(MouseEventType.MIDDLE_UP), 6)
        self.assertEqual(int(MouseEventType.SCROLL), 7)
        self.assertEqual(int(MouseEventType.DRAG), 8)

    def test_enum_str(self):
        """MouseEventType 字符串表示正确"""
        self.assertEqual(str(MouseEventType.MOVE), "MOVE")
        self.assertEqual(str(MouseEventType.SCROLL), "SCROLL")
        self.assertEqual(str(MouseEventType.DRAG), "DRAG")


class TestKeyModifier(unittest.TestCase):
    """测试键盘修饰键位标志"""

    def test_modifier_values(self):
        """KeyModifier 位标志正确"""
        self.assertEqual(int(KeyModifier.NONE), 0)
        self.assertEqual(int(KeyModifier.SHIFT), 1)
        self.assertEqual(int(KeyModifier.CTRL), 2)
        self.assertEqual(int(KeyModifier.ALT), 4)
        self.assertEqual(int(KeyModifier.WIN), 8)

    def test_modifier_combinations(self):
        """KeyModifier 支持位组合"""
        combo = KeyModifier.SHIFT | KeyModifier.CTRL
        self.assertEqual(int(combo), 3)


class TestKeyData(unittest.TestCase):
    """测试键盘事件数据结构"""

    def test_construction(self):
        """KeyData 构造成功"""
        fd = KeyData(
            key_code=65,
            key_name="A",
            is_press=True,
            modifiers=0,
        )
        self.assertEqual(fd.key_code, 65)
        self.assertEqual(fd.key_name, "A")
        self.assertTrue(fd.is_press)
        self.assertIsInstance(fd.timestamp, float)
        self.assertIsInstance(fd.sequence, int)

    def test_to_dict(self):
        """KeyData.to_dict() 正确"""
        fd = KeyData(key_code=65, key_name="A", is_press=True)
        d = fd.to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["key_code"], 65)
        self.assertEqual(d["key_name"], "A")
        self.assertTrue(d["is_press"])

    def test_from_dict(self):
        """KeyData.from_dict() 正确"""
        d = {"key_code": 65, "key_name": "A", "is_press": True, "modifiers": 0}
        fd = KeyData.from_dict(d)
        self.assertEqual(fd.key_code, 65)
        self.assertEqual(fd.key_name, "A")
        self.assertTrue(fd.is_press)

    def test_str_repr(self):
        """KeyData 字符串表示"""
        fd = KeyData(key_code=65, key_name="A", is_press=True)
        self.assertIn("KeyData", repr(fd))
        self.assertIn("A", str(fd))


class TestMouseData(unittest.TestCase):
    """测试鼠标事件数据结构"""

    def test_construction(self):
        """MouseData 构造成功"""
        md = MouseData(
            x=100,
            y=200,
            event_type=int(MouseEventType.MOVE),
        )
        self.assertEqual(md.x, 100)
        self.assertEqual(md.y, 200)
        self.assertEqual(md.event_type, 0)
        self.assertIsInstance(md.timestamp, float)
        self.assertIsInstance(md.sequence, int)

    def test_to_dict(self):
        """MouseData.to_dict() 正确"""
        md = MouseData(x=100, y=200, event_type=0)
        d = md.to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["x"], 100)
        self.assertEqual(d["y"], 200)
        self.assertEqual(d["event_type"], 0)

    def test_from_dict(self):
        """MouseData.from_dict() 正确"""
        d = {"x": 100, "y": 200, "event_type": 0, "button": "none", "delta": 0}
        md = MouseData.from_dict(d)
        self.assertEqual(md.x, 100)
        self.assertEqual(md.y, 200)


class TestKeyboardDispatcher(unittest.TestCase):
    """测试键盘分发器"""

    def test_construction(self):
        """KeyboardDispatcher 构造成功"""
        kbd = KeyboardDispatcher(backend="polling")
        self.assertEqual(kbd.backend_name, "polling")

    def test_lifecycle(self):
        """KeyboardDispatcher start/stop 正常"""
        kbd = KeyboardDispatcher(backend="polling")
        try:
            kbd.start()
            self.assertTrue(kbd.is_running)
            self.assertGreaterEqual(kbd.dispatch_count, 0)
        finally:
            kbd.stop()
        self.assertFalse(kbd.is_running)

    def test_subscribe(self):
        """KeyboardDispatcher.subscribe() 成功"""
        kbd = KeyboardDispatcher(backend="polling")
        try:
            kbd.start()
            received = []

            def on_key(fd):
                received.append(fd)

            sub = kbd.subscribe(on_key)
            time.sleep(0.1)
            self.assertIsNotNone(sub)
            sub.dispose()
        finally:
            kbd.stop()


class TestMouseDispatcher(unittest.TestCase):
    """测试鼠标分发器"""

    def test_construction(self):
        """MouseDispatcher 构造成功"""
        mouse = MouseDispatcher(backend="polling")
        self.assertEqual(mouse.backend_name, "polling")

    def test_lifecycle(self):
        """MouseDispatcher start/stop 正常"""
        mouse = MouseDispatcher(backend="polling")
        try:
            mouse.start()
            self.assertTrue(mouse.is_running)
            self.assertGreaterEqual(mouse.dispatch_count, 0)
        finally:
            mouse.stop()
        self.assertFalse(mouse.is_running)


class TestSimulation(unittest.TestCase):
    """测试模拟操作"""

    def test_keyboard_type_text(self):
        """KeyboardDispatcher.type_text() 不抛异常"""
        kbd = KeyboardDispatcher(backend="polling")
        try:
            kbd.start()
            # type_text 即使无焦点也不应抛异常
            kbd.type_text("hello")
        except Exception as e:
            self.fail(f"type_text 抛异常: {e}")
        finally:
            kbd.stop()

    def test_keyboard_press_release(self):
        """KeyboardDispatcher.press/release() 不抛异常"""
        kbd = KeyboardDispatcher(backend="polling")
        try:
            kbd.start()
            kbd.press("A")
            kbd.release("A")
        except Exception as e:
            self.fail(f"press/release 抛异常: {e}")
        finally:
            kbd.stop()

    def test_mouse_move(self):
        """MouseDispatcher.move_to() 不抛异常"""
        mouse = MouseDispatcher(backend="polling")
        try:
            mouse.start()
            mouse.move_to(500, 300)
        except Exception as e:
            self.fail(f"move_to 抛异常: {e}")
        finally:
            mouse.stop()

    def test_mouse_click(self):
        """MouseDispatcher.click() 不抛异常"""
        mouse = MouseDispatcher(backend="polling")
        try:
            mouse.start()
            mouse.click("left")
        except Exception as e:
            self.fail(f"click 抛异常: {e}")
        finally:
            mouse.stop()

    def test_mouse_scroll(self):
        """MouseDispatcher.scroll() 不抛异常"""
        mouse = MouseDispatcher(backend="polling")
        try:
            mouse.start()
            mouse.scroll(120)
        except Exception as e:
            self.fail(f"scroll 抛异常: {e}")
        finally:
            mouse.stop()


class TestTopLevelFactory(unittest.TestCase):
    """测试顶层工厂函数"""

    def test_from_keyboard(self):
        """from_keyboard() 返回正确类型"""
        try:
            obs, disp = from_keyboard(backend="polling")
            self.assertTrue(hasattr(obs, "subscribe"))
            self.assertTrue(hasattr(disp, "start"))
            self.assertTrue(hasattr(disp, "stop"))
            disp.stop()
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装，使用 Python 回退")

    def test_from_mouse(self):
        """from_mouse() 返回正确类型"""
        try:
            obs, disp = from_mouse(backend="polling")
            self.assertTrue(hasattr(obs, "subscribe"))
            self.assertTrue(hasattr(disp, "start"))
            disp.stop()
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")


class TestSubject(unittest.TestCase):
    """测试 Subject"""

    def test_key_subject(self):
        """KeySubject 构造成功"""
        try:
            with KeySubject(backend="polling") as subject:
                self.assertTrue(subject.is_running)
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")

    def test_mouse_subject(self):
        """MouseSubject 构造成功"""
        try:
            with MouseSubject(backend="polling") as subject:
                self.assertTrue(subject.is_running)
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")


class TestObserver(unittest.TestCase):
    """测试 Observer"""

    def test_key_observer_construction(self):
        """KeyObserver 构造成功"""
        pressed_keys = []

        def on_press(fd):
            pressed_keys.append(fd.key_name)

        obs = KeyObserver(on_press=on_press)
        self.assertIsNotNone(obs)

    def test_mouse_observer_construction(self):
        """MouseObserver 构造成功"""
        moves = []

        def on_move(fd):
            moves.append((fd.x, fd.y))

        obs = MouseObserver(on_move=on_move)
        self.assertIsNotNone(obs)


class TestWriteOperators(unittest.TestCase):
    """测试写入操作符"""

    def test_write_to_keyboard_op(self):
        """write_to_keyboard() 返回操作符"""
        try:
            _, disp = from_keyboard(backend="polling")
            op = write_to_keyboard(disp)
            self.assertTrue(callable(op))
            disp.stop()
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")

    def test_write_to_mouse_op(self):
        """write_to_mouse() 返回操作符"""
        try:
            _, disp = from_mouse(backend="polling")
            op = write_to_mouse(disp)
            self.assertTrue(callable(op))
            disp.stop()
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")


# ============================================================================
# 新增 API 测试
# ============================================================================


class TestKeyDataNewFields(unittest.TestCase):
    """测试 KeyData 新字段"""

    def test_event_type_field(self):
        """KeyData 有 event_type 字段"""
        fd = KeyData(key_code=65, is_press=True)
        self.assertEqual(fd.event_type, KeyEventType.KEY_DOWN)

        fd2 = KeyData(key_code=65, is_press=False)
        self.assertEqual(fd2.event_type, KeyEventType.KEY_UP)

    def test_window_title_field(self):
        """KeyData 有 window_title 字段"""
        fd = KeyData(key_code=65, is_press=True, window_title="TestWindow")
        self.assertEqual(fd.window_title, "TestWindow")

    def test_timestamp_milliseconds(self):
        """KeyData timestamp 是毫秒"""
        fd = KeyData(key_code=65, is_press=True)
        # 毫秒应该是 > 1700000000000 (2023年后)
        self.assertGreater(fd.timestamp, 1700000000000)

    def test_to_dict_new_fields(self):
        """KeyData.to_dict() 包含新字段"""
        fd = KeyData(key_code=65, is_press=True)
        d = fd.to_dict()
        self.assertIn("event_type", d)
        self.assertIn("event_type_name", d)
        self.assertIn("window_title", d)


class TestKeyboardDispatcherNewFeatures(unittest.TestCase):
    """测试 KeyboardDispatcher 新功能"""

    def test_subject_property(self):
        """KeyboardDispatcher 有 subject 属性"""
        kbd = KeyboardDispatcher(backend="polling")
        self.assertTrue(hasattr(kbd, "subject"))

    def test_tap_method(self):
        """KeyboardDispatcher.tap() 不抛异常"""
        kbd = KeyboardDispatcher(backend="polling")
        try:
            kbd.start()
            kbd.tap("A")
        except Exception as e:
            self.fail(f"tap 抛异常: {e}")
        finally:
            kbd.stop()

    def test_self_filtered_count(self):
        """KeyboardDispatcher 有 self_filtered_count 属性"""
        kbd = KeyboardDispatcher(backend="polling")
        self.assertTrue(hasattr(kbd, "self_filtered_count"))
        self.assertEqual(kbd.self_filtered_count, 0)


class TestMouseDispatcherNewFeatures(unittest.TestCase):
    """测试 MouseDispatcher 新功能"""

    def test_subject_property(self):
        """MouseDispatcher 有 subject 属性"""
        mouse = MouseDispatcher(backend="polling")
        self.assertTrue(hasattr(mouse, "subject"))

    def test_double_click(self):
        """MouseDispatcher.double_click() 不抛异常"""
        mouse = MouseDispatcher(backend="polling")
        try:
            mouse.start()
            mouse.double_click("left")
        except Exception as e:
            self.fail(f"double_click 抛异常: {e}")
        finally:
            mouse.stop()

    def test_move_relative(self):
        """MouseDispatcher.move_relative() 不抛异常"""
        mouse = MouseDispatcher(backend="polling")
        try:
            mouse.start()
            mouse.move_relative(10, 10)
        except Exception as e:
            self.fail(f"move_relative 抛异常: {e}")
        finally:
            mouse.stop()

    def test_self_filtered_count(self):
        """MouseDispatcher 有 self_filtered_count 属性"""
        mouse = MouseDispatcher(backend="polling")
        self.assertTrue(hasattr(mouse, "self_filtered_count"))


class TestSubjectNewFeatures(unittest.TestCase):
    """测试 Subject 新功能"""

    def test_key_subject_dispatch_count(self):
        """KeySubject 有 dispatch_count 属性"""
        try:
            with KeySubject(backend="polling") as ks:
                self.assertTrue(hasattr(ks, "dispatch_count"))
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")

    def test_key_subject_tap(self):
        """KeySubject.tap() 不抛异常"""
        try:
            with KeySubject(backend="polling") as ks:
                ks.tap("A")
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")

    def test_mouse_subject_double_click(self):
        """MouseSubject.double_click() 不抛异常"""
        try:
            with MouseSubject(backend="polling") as ms:
                ms.double_click("left")
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")

    def test_mouse_subject_move_relative(self):
        """MouseSubject.move_relative() 不抛异常"""
        try:
            with MouseSubject(backend="polling") as ms:
                ms.move_relative(10, 10)
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")


class TestFactoryNewParams(unittest.TestCase):
    """测试工厂函数新参数"""

    def test_from_keyboard_auto_start_false(self):
        """from_keyboard(auto_start=False) 不自动启动"""
        try:
            obs, disp = from_keyboard(backend="polling", auto_start=False)
            self.assertFalse(disp.is_running)
            disp.start()
            self.assertTrue(disp.is_running)
            disp.stop()
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")

    def test_from_mouse_auto_start_false(self):
        """from_mouse(auto_start=False) 不自动启动"""
        try:
            obs, disp = from_mouse(backend="polling", auto_start=False)
            self.assertFalse(disp.is_running)
            disp.stop()
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")


class TestObserverNewFeatures(unittest.TestCase):
    """测试 Observer 新功能"""

    def test_key_observer_on_error(self):
        """KeyObserver 有 on_error 参数"""
        errors = []
        obs = KeyObserver(on_error=lambda e: errors.append(e))
        self.assertIsNotNone(obs)

    def test_key_observer_attach(self):
        """KeyObserver.attach() 返回 self"""
        try:
            with KeySubject(backend="polling") as ks:
                obs = KeyObserver(on_press=lambda kd: None)
                result = obs.attach(ks)
                self.assertIsNotNone(result)
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")

    def test_key_observer_unsubscribe(self):
        """KeyObserver.unsubscribe() 正常工作"""
        try:
            ks = KeySubject(backend="polling")
            ks.start()
            obs = KeyObserver(on_press=lambda kd: None)
            obs.subscribe(ks)
            self.assertTrue(obs.is_subscribed)
            obs.unsubscribe()
            self.assertFalse(obs.is_subscribed)
            ks.stop()
        except NotImplementedError:
            self.skipTest("Rust 扩展未安装")

    def test_mouse_observer_on_error(self):
        """MouseObserver 有 on_error 参数"""
        obs = MouseObserver(on_error=lambda e: None)
        self.assertIsNotNone(obs)


class TestPickleSerialization(unittest.TestCase):
    """测试 Pickle 序列化"""

    def test_key_data_pickle(self):
        """KeyData.to_pickle/from_pickle 正常工作"""
        fd = KeyData(key_code=65, is_press=True)
        try:
            b = fd.to_pickle()
            fd2 = KeyData.from_pickle(b, trusted=True)
            self.assertEqual(fd.key_code, fd2.key_code)
            self.assertEqual(fd.is_press, fd2.is_press)
        except AttributeError:
            self.skipTest("pickle 方法未实现")

    def test_mouse_data_pickle(self):
        """MouseData.to_pickle/from_pickle 正常工作"""
        md = MouseData(x=100, y=200, event_type=0)
        try:
            b = md.to_pickle()
            md2 = MouseData.from_pickle(b, trusted=True)
            self.assertEqual(md.x, md2.x)
            self.assertEqual(md.y, md2.y)
        except AttributeError:
            self.skipTest("pickle 方法未实现")


# ============================================================================
# 主入口
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("rx_rust.keyboard_mouse 集成测试")
    print("=" * 60)

    if not USE_RUST:
        print("\n[WARNING] Rust 扩展未加载，部分测试将被跳过")
        print("[HINT]  运行 `maturin develop` 或 `pip install .` 安装 Rust 扩展\n")

    # 运行测试
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # 总结
    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("[PASS] 所有测试通过!")
    else:
        print(f"[FAIL] {len(result.failures)} 失败, {len(result.errors)} 错误")
        for test, trace in result.failures:
            print(f"\n--- 失败: {test} ---")
            print(trace)
        for test, trace in result.errors:
            print(f"\n--- 错误: {test} ---")
            print(trace)
    print("=" * 60)

    sys.exit(0 if result.wasSuccessful() else 1)
