# Rx-Rust — 高性能响应式编程库

> **版本 0.1.0** — 基于 Rust 实现的响应式编程库，并提供 PyO3 的 Python 绑定（rxpy）。

[![Rust](https://img.shields.io/badge/Rust-1.75%2B-DEA584?logo=rust)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-62%20%2F%2062%20%E2%9C%85-success)](rx-rust/tests/)
[![Version](https://img.shields.io/badge/version-0.1.0-orange)](rx-rust/Cargo.toml)

---

## 项目总览

Rx-Rust 是一个基于 Rust 的响应式编程（Reactive Programming）库，灵感来自微软的 Rx.NET 和 Python 的 RxPY。它提供了完整的 Observable/Observer/Subject/Scheduler 系统，支持创建、转换、过滤、组合、数学、时间、错误处理、多播等 **50+ 操作符**，并通过 **PyO3** 提供高性能的 Python 绑定（`rxpy`）。

```text
          ┌──────────────────────────────┐
          │  rx-rust (Rust 核心库)       │
          │  - Observable/Observer       │
          │  - 50+ 操作符                │
          │  - Subject (3 种)            │
          │  - Scheduler (4 种)          │
          │  - 62 个单元测试 ✅          │
          └────────────┬─────────────────┘
                       │
                   PyO3 绑定
                       │
         ┌─────────────┴──────────────┐
         │  rxpy (Python 包)           │
         │  - Observable / Subject     │
         │  - Subscription             │
         │  - 完整的中文文档           │
         └─────────────────────────────┘
```

## 仓库地址

- **GitCode**: `https://gitcode.com/VictorTop/Rx-Rust`
- **PyPI**: `pip install rxpy`

---

## 目录结构

```text
Rx-Rust/
├── rx-rust/             # Rust 核心库
│   ├── src/
│   │   ├── lib.rs      # 库入口 + prelude
│   │   ├── observable/ # Observable 定义与创建函数
│   │   ├── observer/   # Observer trait + FnObserver
│   │   ├── operators/  # 所有操作符实现（50+）
│   │   ├── subject/    # Publish/Behavior/ReplaySubject
│   │   ├── scheduler/  # 4 种调度器
│   │   └── subscription/ # Subscription + Disposable
│   ├── tests/          # 集成测试（62 个）
│   ├── Cargo.toml      # 包配置
│   └── README.md       # Rust 侧文档
├── rxpy/                # Python 绑定
│   ├── src/lib.rs     # PyO3 绑定代码
│   ├── python/rxpy/__init__.py  # Python 层 + docstring
│   ├── Cargo.toml
│   ├── pyproject.toml  # maturin 配置
│   └── README.md       # Python 侧文档
├── GUIDE.md            # 📖 完整使用指南（强烈推荐阅读）
├── LICENSE             # MIT 许可证
└── README.md           # 本文件
```

---

## 核心能力一览

### 🎯 操作符（50+）

| 分类 | 操作符 |
|------|--------|
| **创建** | `of` · `from_iter` · `empty` · `never` · `throw` · `range` · `repeat` · `defer` · `generate` |
| **转换** | `map` · `flat_map` · `scan` · `reduce` · `buffer` |
| **过滤** | `filter` · `take` · `skip` · `first` · `last` · `take_while` · `skip_while` · `distinct` · `distinct_until_changed` · `element_at` · `take_last` · `default_if_empty` · `contains` · `ignore_elements` |
| **组合** | `merge` · `concat` · `zip` · `combine_latest` · `switch_map` |
| **数学** | `count` · `sum` · `min` · `max` · `average` |
| **时间** | `debounce` · `throttle` · `timeout` |
| **错误** | `catch_error` · `on_error_resume_next` · `retry` · `retry_when` |
| **调度** | `subscribe_on` · `observe_on` |
| **多播** | `publish` · `ConnectableObservable` |

### 📡 Subject（主题）

- **`PublishSubject`** — 广播型主题，新订阅者只能收到订阅之后的值
- **`BehaviorSubject`** — 带当前值的主题，新订阅者立即收到最新值
- **`ReplaySubject`** — 缓存并重放历史值给新订阅者
- **`ConnectableObservable`** — 延迟启动的热 Observable（通过 `publish()` 创建）

### 🔄 调度器

- **`CurrentThreadScheduler`** — 在当前线程同步执行
- **`ImmediateScheduler`** — 立即执行，不做调度
- **`ThreadPoolScheduler(n)`** — n 线程的线程池并发执行
- **`AsyncScheduler`** — 异步任务调度

---

## 快速开始

### Rust（作为库使用）

```toml
# Cargo.toml
[dependencies]
rx-rust = { path = "./rx-rust", version = "0.1.0" }
```

```rust
use rx_rust::prelude::*;

fn main() {
    // 1. 创建: 从 1 到 10
    let source = range::<i32, ()>(1, 10);

    // 2. 管道: 偶数 → 平方 → 取前 3 个
    source
        .filter(|n| n % 2 == 0)
        .map(|n| n * n)
        .take(3)
        .subscribe(FnObserver::new(
            |v| println!("值: {:?}", v),
            || println!("完成！"),
        ));

    // 输出:
    // 值: Ok(4)
    // 值: Ok(16)
    // 值: Ok(36)
    // 完成！
}
```

### Python（通过 rxpy 包）

```bash
# 安装
pip install rxpy
```

```python
import rxpy

# 创建 + 管道 + 订阅
rxpy.Observable.range(1, 10) \
    .filter(lambda x: x % 2 == 0) \
    .map(lambda x: x * x) \
    .subscribe(
        on_next=lambda v: print(f"值: {v}"),
        on_completed=lambda: print("完成！"),
    )
```

---

## 测试状态

```text
$ cargo test
running 62 tests
test integration::observable::test_of ... ok
test integration::observable::test_from_iter ... ok
test integration::observable::test_filter ... ok
test integration::observable::test_map ... ok
test integration::observable::test_take ... ok
test integration::observable::test_skip ... ok
test integration::observable::test_first ... ok
test integration::observable::test_last ... ok
test integration::observable::test_take_while ... ok
test integration::observable::test_skip_while ... ok
test integration::observable::test_distinct_until_changed ... ok
test integration::observable::test_element_at ... ok
test integration::observable::test_flat_map ... ok
test integration::observable::test_scan ... ok
test integration::observable::test_reduce ... ok
test integration::observable::test_collect ... ok
test integration::observable::test_buffer ... ok
test integration::observable::test_merge ... ok
test integration::observable::test_concat ... ok
test integration::observable::test_zip ... ok
test integration::observable::test_combine_latest ... ok
test integration::observable::test_publish_subject ... ok
test integration::observable::test_behavior_subject ... ok
test integration::observable::test_replay_subject ... ok
test integration::observable::test_timeout ... ok
test integration::observable::test_default_if_empty ... ok
test integration::observable::test_contains ... ok
test integration::observable::test_ignore_elements ... ok
test integration::observable::test_all ... ok
test integration::observable::test_take_last ... ok
test integration::observable::test_distinct ... ok
test integration::observable::test_catch_error ... ok
test integration::observable::test_on_error_resume_next ... ok
test integration::observable::test_retry ... ok
test integration::observable::test_retry_when ... ok
test integration::observable::test_switch_map ... ok
test integration::observable::test_debounce ... ok
test integration::observable::test_throttle ... ok
test integration::observable::test_publish ... ok
test integration::observable::test_connectable ... ok
test integration::observable::test_observe_on ... ok
test integration::observable::test_subscribe_on ... ok
test integration::observable::test_count ... ok
test integration::observable::test_sum ... ok
test integration::observable::test_min ... ok
test integration::observable::test_max ... ok
test integration::observable::test_average ... ok
test integration::observable::test_range ... ok
test integration::observable::test_repeat ... ok
test integration::observable::test_empty ... ok
test integration::observable::test_never ... ok
test integration::observable::test_throw ... ok
test integration::observable::test_defer ... ok
test integration::subscription::test_subscription_basic ... ok
test integration::subscription::test_subscription_dispose ... ok
test integration::subscription::test_disposable ... ok
test integration::subscription::test_composite_disposable ... ok

test result: ok. 62 passed; 0 failed; 0 ignored; 0 measured
```

---

## 📖 更多文档

- **[GUIDE.md](GUIDE.md)** — 完整的使用指南！包含：
  - 所有操作符的详细说明和代码示例
  - 三个实战案例（日志分析、防抖搜索、事件总线）
  - 最佳实践和常见陷阱
  - Rust API 参考
  - Python API 参考
  - 调试与排错指南
- **[rx-rust/README.md](rx-rust/README.md)** — Rust 侧库说明
- **[rxpy/README.md](rxpy/README.md)** — Python 侧包说明

---

## 构建与运行

### 构建 Rust 库

```bash
cd rx-rust
cargo build           # 调试构建
cargo build --release # 发布构建
cargo test            # 运行测试
```

### 构建 Python 包（使用 maturin）

```bash
cd rxpy
pip install maturin

# 开发模式（直接可用）
maturin develop

# 构建 wheel（用于发布）
maturin build --release
# 产物在 target/wheels/ 目录
```

---

## 许可

**MIT License** © 2025 — 请参阅 [LICENSE](LICENSE) 文件以获取完整的许可证文本。

---

## TODO / 未来规划

- [ ] `interval` / `timer` 定时发射操作符
- [ ] `group_by` / `window` 分组操作符
- [ ] `fork_join` 组合操作符
- [ ] `retry_with_backoff` 指数退避重试
- [ ] `share` / `ref_count` 便捷多播 API
- [ ] WebAssembly (WASM) 支持
- [ ] `async/await` 一流集成（Future → Observable 桥接）
- [ ] 更多 Python 操作符绑定（当前已覆盖核心路径）
- [ ] 性能基准测试
- [ ] 更多文档和示例
