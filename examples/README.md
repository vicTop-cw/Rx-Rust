# Rx-Rust 示例

本目录包含 Rx-Rust 库的使用示例。

## 文件列表

| 文件 | 描述 |
|------|------|
| `basic_usage.py` | Observable、Observer、Subject 基础用法 |
| `keyboard_example.py` | 键盘监控与模拟示例 |
| `mouse_example.py` | 鼠标监控与模拟示例 |

## 运行示例

### 基础用法
```bash
python basic_usage.py
```

### 键盘示例
```bash
# 基础键盘监控
python keyboard_example.py monitor

# 使用 KeyObserver
python keyboard_example.py observer

# 组合键监听
python keyboard_example.py hotkey

# 键盘模拟
python keyboard_example.py simulate
```

### 鼠标示例
```bash
# 基础鼠标监控
python mouse_example.py monitor

# 使用 MouseObserver
python mouse_example.py observer

# 点击计数
python mouse_example.py count

# 鼠标模拟
python mouse_example.py simulate
```

## 注意事项

- 键盘/鼠标监控需要先编译 Rust 扩展 (`maturin develop`)
- Windows Hook 模式需要管理员权限
- Polling 模式适用于所有用户