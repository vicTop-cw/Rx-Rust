# Rx-Rust 测试目录

本目录包含所有测试文件。

## 文件列表

| 文件 | 描述 |
|------|------|
| `test_keyboard_mouse.py` | 键盘鼠标模块测试 |
| `test_clipboard.py` | 剪贴板模块测试 |
| `test_file_watcher.py` | 文件监控测试 |
| `test_rx_rust.py` | 核心库测试 |
| `_verify_operators.py` | 操作符验证脚本 |
| `test_dispatch_debug*.py` | 分发器调试测试 |

## 子目录

| 目录 | 描述 |
|------|------|
| `from_pypi/` | PyPI 安装后的测试 |

## 运行测试

```bash
# 运行所有测试
pytest tests/

# 运行单个测试
pytest tests/test_keyboard_mouse.py

# 运行 PyPI 测试
pytest tests/from_pypi/
```