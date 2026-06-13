# RxPY

> Reactive Extensions for Python — powered by Rust 🦀

[![Rust](https://img.shields.io/badge/Rust-1.75+-DEA584.svg?logo=rust)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB.svg?logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.1.0-orange.svg)](pyproject.toml)

RxPY 是一个用于组合异步和基于事件的程序的 Python 库，灵感来自微软的 Reactive Extensions (Rx)。
它通过 **PyO3** 绑定高性能 **Rust** 实现，提供零开销的响应式编程体验。

---

## ✨ 特性

### 核心类型
- **`Observable`** — 在未来可能发射 0 个或多个值的惰性序列
- **`Subscription`** — 订阅句柄，支持 `dispose()` 取消订阅
- **`Observer`** — 订阅者回调（`on_next` / `on_completed`）

### Subjects（广播器）
- **`PublishSubject`** — 广播型主题，向所有订阅者发送值
- **`BehaviorSubject`** — 带"当前值"的主题，新订阅者会立即收到最新值
- **`ReplaySubject`** — 重放历史值的主题，缓存最近 N 个值

### 调度器
- **`CurrentThreadScheduler`** — 在当前线程同步执行
- **`ImmediateScheduler`** — 立即执行，不做调度
- **`ThreadPoolScheduler`** — 在线程池中并发执行
- **`AsyncScheduler`** — 异步执行

### 操作符（全部方法链式可组合）
| 类别 | 操作符 |
|------|--------|
| **创建** | `of`, `from_iter`, `range`, `repeat`, `empty`, `never` |
| **转换** | `map`, `flat_map`, `scan`, `reduce` |
| **过滤** | `filter`, `take`, `skip`, `first`, `take_while`, `skip_while`, `distinct`, `distinct_until_changed`, `element_at`, `default_if_empty`, `ignore_elements` |
| **组合** | `merge`, `concat`, `zip`, `combine_latest`, `switch_map` |
| **数学** | `count`, `sum`, `min`, `max`, `average` |
| **时间** | `debounce`, `throttle`, `timeout` |
| **错误** | `catch_error`, `on_error_resume_next`, `retry`, `retry_when` |
| **调度** | `subscribe_on`, `observe_on` |

---

## 🚀 快速开始

### 安装

```bash
# 从 PyPI 安装（推荐）
pip install rxpy

# 或者从源码构建（需要 Rust 工具链）
git clone https://gitcode.com/VictorTop/Rx-Rust.git
cd Rx-Rust/rxpy
pip install maturin
maturin develop
```

### 三分钟第一个程序

```python
import rxpy

# 1. 创建: 从 1 到 5 的序列
observable = rxpy.Observable.range(1, 5)

# 2. 管道: 过滤偶数 + 平方
processed = (
    observable
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x * x)
)

# 3. 订阅并打印
sub = processed.subscribe(
    on_next=lambda v: print(f"收到: {v}"),
    on_completed=lambda: print("完成！"),
)

# 也可以用上下文管理器自动取消
# with processed.subscribe(on_next=print) as sub:
#     pass  # 退出时自动 sub.dispose()
```

**运行结果:**
```text
收到: 4
收到: 16
完成！
```

---

## 📚 使用指南

### 1. Subject 示例：事件总线

```python
import rxpy

# 创建一个发布主题
subject = rxpy.PublishSubject()

# 订阅者 A
result_a = []
subject.subscribe(
    on_next=lambda v: result_a.append(("A", v)),
    on_completed=lambda: result_a.append("A done"),
)

# 发射值
subject.on_next(1)  # A 收到 1
subject.on_next(2)  # A 收到 2

# 订阅者 B（迟到的订阅者）
result_b = []
subject.subscribe(
    on_next=lambda v: result_b.append(("B", v)),
    on_completed=lambda: result_b.append("B done"),
)

subject.on_next(3)  # A 和 B 都收到 3
subject.on_completed()  # A 和 B 都收到完成信号

print(result_a)  # [("A", 1), ("A", 2), ("A", 3), "A done"]
print(result_b)  # [("B", 3), "B done"]
```

### 2. BehaviorSubject：带状态的主题

```python
import rxpy

# 初始值为 0
subject = rxpy.BehaviorSubject(0)

# 订阅者 A 立即收到 0
result_a = []
subject.subscribe(on_next=lambda v: result_a.append(v))

subject.on_next(1)  # A 收到 1
subject.on_next(2)  # A 收到 2

# 订阅者 B 立即收到当前值 2，以及后续值
result_b = []
subject.subscribe(on_next=lambda v: result_b.append(v))

subject.on_next(3)  # A 和 B 都收到 3

print(result_a)  # [0, 1, 2, 3]
print(result_b)  # [2, 3]
```

### 3. ReplaySubject：重放历史

```python
import rxpy

# 缓存最近 2 个值
subject = rxpy.ReplaySubject(2)

subject.on_next(1)
subject.on_next(2)
subject.on_next(3)

# 订阅者会收到缓存的 2 和 3
result = []
subject.subscribe(on_next=lambda v: result.append(v))
subject.on_completed()

print(result)  # [2, 3]
```

### 4. 数学与聚合

```python
import rxpy

# 求和
result = (
    rxpy.Observable.from_iter([1, 2, 3, 4, 5])
    .reduce(0, lambda acc, x: acc + x)
    .collect()
)
print(result)  # [15]

# 使用 collect() 直接收集所有值到列表
values = rxpy.Observable.range(1, 5).collect()
print(values)  # [1, 2, 3, 4, 5]
```

### 5. 过滤与转换管道

```python
import rxpy

result = (
    rxpy.Observable.from_iter(range(1, 11))  # 1..10
    .filter(lambda x: x % 2 == 0)              # 只保留偶数: 2, 4, 6, 8, 10
    .map(lambda x: x * 2)                       # 翻倍: 4, 8, 12, 16, 20
    .take(3)                                    # 只取前 3 个: 4, 8, 12
    .collect()
)

print(result)  # [4, 8, 12]
```

---

## 🏗️ 架构

```text
                 +-------------------+
                 |     Python API     |
                 |  (rxpy/__init__.py)|
                 +---------+---------+
                            |
                           绑定
                            |
                 +---------v---------+
                 |     PyO3 FFI      |
                 |  (Cargo.toml +    |
                 |   src/lib.rs)      |
                 +---------+---------+
                            |
                         调用
                            |
    +------------------------v------------------------+
    |              rx-rust (Rust 核心库)              |
    |  src/lib.rs + src/observable + src/operators/  |
    |  + src/subject + src/scheduler + src/observer  |
    +------------------------+------------------------+
                             |
                       62 个单元测试
                            |
                   +--------v--------+
                   |  cargo test ✅  |
                   +-----------------+
```

RxPY 构建在 Rust 库 `rx-rust` 之上，所有核心逻辑（Observable、操作符、Subject、调度器）均由 Rust 实现，并通过 PyO3 暴露给 Python。

---

## 📖 更多文档

- **[GUIDE.md](../GUIDE.md)** — 完整使用指南，从入门到进阶，包含 Rust 和 Python 两个 API 的详细说明
- **[README.md](../README.md)** — 项目总览

---

## 🔧 开发与测试

### 环境要求
- Rust 1.75+
- Python 3.8+
- Maturin

### 构建

```bash
cd rxpy
pip install maturin
maturin develop  # 开发模式安装
```

### 测试

```bash
# Rust 测试（在 rx-rust 目录下）
cd ../rx-rust
cargo test

# Python 测试（在 rxpy 目录下）
cd ../rxpy
python -c "
import rxpy
# 基本功能测试
result = rxpy.Observable.range(1, 5).filter(lambda x: x % 2 == 0).map(lambda x: x * 10).collect()
print('PASS:', result == [20, 40])
"
```

---

## 📄 许可证

MIT License © RxPY Contributors

请查看 [LICENSE](LICENSE) 文件以获取完整的许可证文本。
