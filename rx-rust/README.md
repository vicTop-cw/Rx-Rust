# rx_rust

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![version](https://img.shields.io/badge/version-0.1.0-blue.svg)]()
[![Tests](https://img.shields.io/badge/tests-62%20passed-brightgreen.svg)]()

> 一个纯 Rust 实现的响应式编程（Reactive Programming）库，灵感来自 RxJava / RxJS。

---

## 目录

- [项目简介](#项目简介)
- [特性](#特性)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [操作符大全](#操作符大全)
- [Subject 实现](#subject-实现)
- [调度器系统](#调度器系统)
- [错误处理](#错误处理)
- [示例代码](#示例代码)
- [高级用法](#高级用法)
- [常见问题](#常见问题)
- [贡献](#贡献)
- [许可证](#许可证)

---

## 项目简介

**rx_rust** 是一个完整的响应式编程库，让你能够以声明式（declarative）的方式处理异步数据流和事件。它提供了与 Rx 家族（RxJava、RxJS、Rx.NET）一致的 API 设计，包括：

- **Observable** - 可观察的数据流
- **Observer** - 订阅并消费数据的观察者
- **Operators** - 丰富的转换/过滤/组合操作符
- **Subject** - 既是 Observable 又是 Observer 的桥接器
- **Scheduler** - 灵活的线程/任务调度系统

```text
      +----------------+      +-----------------+      +-------------------+
      |   Observable   | ---> |    Operators    | ---> |    Observer       |
      |   (数据源)      |      |  (map/filter...) |     |  (订阅者：on_next) |
      +----------------+      +-----------------+      +-------------------+
```

---

## 特性

### ✅ 完整的响应式原语

- **创建型**: `of`, `from_iter`, `range`, `repeat`, `empty`, `never`, `throw`
- **转换型**: `map`, `flat_map`, `scan`, `reduce`, `buffer`
- **过滤型**: `filter`, `take`, `take_last`, `take_while`, `skip`, `skip_while`, `first`, `last`, `distinct`, `distinct_until_changed`, `contains`
- **数学型**: `reduce`, `count`, `sum`, `min`, `max`, `average`
- **组合型**: `merge`, `concat`, `zip`, `combine_latest`, `switch_map`
- **错误处理**: `catch_error`, `on_error_resume_next`, `retry`, `retry_when`
- **时间型**: `debounce`, `throttle`, `timeout`
- **调度**: `observe_on`, `subscribe_on`

### ✅ 线程安全

- 所有核心类型都 `Send + Sync`
- 可安全地跨线程传递订阅
- 基于 `Arc` + `Mutex` 的并发控制

### ✅ 资源管理

- `Subscription` 的 `dispose()` 方法可随时取消订阅
- 自动清理：完成/错误后资源自动释放
- 支持自定义 `Disposable`

### ✅ 纯 Rust，零外部依赖

- 核心功能无需任何依赖
- 可选 `tokio` feature 用于高级异步场景

---

## 快速开始

### 添加依赖

在你的 `Cargo.toml` 中添加：

```toml
[dependencies]
rx_rust = "0.1.0"
```

### 最小示例

```rust
use rx_rust::observable::Observable;
use rx_rust::operators::ObservableExt;
use rx_rust::observer::Observer;

fn main() {
    // 1. 创建 Observable
    let observable = rx_rust::observable::base::from_iter::<i32, ()>(vec![1, 2, 3, 4, 5]);

    // 2. 链式应用操作符
    let filtered = observable
        .filter(|x| x % 2 == 0)      // 过滤偶数
        .map(|x| x * 10)             // 每个值 × 10
        .take(2);                    // 只取前 2 个

    // 3. 订阅并消费数据
    filtered.subscribe(rx_rust::observer::FnObserver::new(
        |value| println!("收到: {:?}", value),
        || println!("完成！"),
    ));
}
// 输出:
// 收到: Ok(20)
// 收到: Ok(40)
// 完成！
```

### 使用 prelude（推荐）

```rust
use rx_rust::prelude::*;

fn main() {
    range::<i32, ()>(1, 5)
        .filter(|x| x % 2 == 1)
        .map(|x| x * x)
        .subscribe(FnObserver::new(
            |v| println!("值: {:?}", v),
            || println!("全部完成"),
        ));
}
```

---

## 核心概念

### Observable &lt;T, E&gt;

**Observable** 是一个可观察的数据流，它可以在未来的某个时刻发射 `T` 类型的值，或发射 `E` 类型的错误，或发射完成信号。

```rust
pub trait Observable<T, E> {
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription;
}
```

Observable 是**惰性**的（Lazy）：你不 `subscribe()`，它就不会产生任何值。

### Observer &lt;T, E&gt;

**Observer** 订阅 Observable 并接收它的通知：

```rust
pub trait Observer<T, E> {
    fn on_next(&self, value: Result<T, E>);  // 收到值或错误
    fn on_completed(&self);                   // 收到完成信号
}
```

最简单的方式是使用 `FnObserver`：

```rust
FnObserver::new(
    |value: Result<i32, ()>| { /* 处理值 */ },
    || { /* 处理完成 */ },
)
```

### Subscription

`subscribe()` 返回的 `Subscription` 可用来**取消订阅**：

```rust
let sub = range::<i32, ()>(1, 100)
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || println!("完成"),
    ));

// 立即取消，Observable 将停止发射
sub.dispose();
```

你也可以将多个 Subscription 组合成一个：

```rust
let composite = Subscription::empty();
// 订阅多个 observable，将它们的 subscription 组合
composite.add(...);
composite.add(...);
// 一次性取消所有
composite.dispose();
```

---

## 操作符大全

### 创建型（Creation）

| 函数 | 说明 |
|------|------|
| `of(value)` | 发射一个值后完成 |
| `from_iter(iter)` | 逐个发射可迭代对象中的值 |
| `range(start, count)` | 发射 `start, start+1, ...` 共 `count` 个值 |
| `repeat(value, count)` | 将一个值重复 `count` 次 |
| `empty()` | 什么都不发射，直接完成 |
| `never()` | 什么都不发射，也不完成（无限等待） |
| `throw(error)` | 发射一个错误后完成 |
| `defer(factory)` | 延迟创建 Observable（直到订阅时才调用 factory） |
| `generate(init, f)` | 用生成函数产生无限序列 |

### 转换型（Transformation）

| 方法 | 签名示例 | 说明 |
|------|---------|------|
| `map(f)` | `f: Fn(T) -> R` | 一对一转换每个值 |
| `flat_map(f)` | `f: Fn(T) -> Observable<R, E>` | 转换并展平嵌套的 Observable |
| `scan(init, f)` | `f: Fn(Acc, T) -> Acc` | 累积并发射每一步的中间结果 |
| `reduce(init, f)` | `f: Fn(Acc, T) -> Acc` | 累积并在完成时发射最终结果 |
| `buffer(n)` | `n: usize` | 将每 n 个值打包成一个 Vec |

### 过滤型（Filtering）

| 方法 | 说明 |
|------|------|
| `filter(pred)` | 只保留满足条件的值 |
| `take(n)` | 只取前 n 个 |
| `skip(n)` | 跳过前 n 个 |
| `take_last(n)` | 只取最后 n 个 |
| `last()` | 只取最后一个 |
| `first()` | 只取第一个 |
| `take_while(pred)` | 取到条件失败为止 |
| `skip_while(pred)` | 跳过直到条件失败 |
| `element_at(n)` | 只取第 n 个值（0-indexed） |
| `distinct()` | 去除重复值 |
| `distinct_until_changed()` | 去除连续重复值 |
| `default_if_empty(default)` | 如果空则发射 default |
| `contains(target)` | 若含有 target 则发射 true，否则 false |
| `ignore_elements()` | 忽略所有值，只保留完成/错误 |

### 数学型（Mathematical）

| 方法 | 说明 |
|------|------|
| `count()` | 发射值的个数 |
| `sum()` | 发射所有值的和 |
| `min()` | 发射最小值 |
| `max()` | 发射最大值 |
| `average()` | 发射平均值 |

### 组合型（Combining）

| 方法 | 说明 |
|------|------|
| `merge(other)` | 合并两个 Observable，任意一个有值都立即发射 |
| `concat(other)` | 按顺序连接，先等第一个完成再开始第二个 |
| `zip(other, f)` | 将两个 Observable 的第 n 个值配对 |
| `combine_latest(other, f)` | 任意一个有新值时，都用"各自最新的一个"配对 |
| `switch_map(f)` | 当上游有新值时，取消上一个内部 Observable 的订阅，切换到新的 |

### 时间型（Time）

| 方法 | 说明 |
|------|------|
| `debounce(duration)` | 安静 `duration` 时间后才发射最后一个值（防抖） |
| `throttle(duration)` | 每个时间窗口内只发射第一个值（节流） |
| `timeout(duration)` | 如果 `duration` 内没有新值，就发出错误 |

### 错误处理型

| 方法 | 说明 |
|------|------|
| `catch_error(f)` | 捕获错误并用 f 返回的 Observable 替代 |
| `on_error_resume_next(other)` | 错误发生后切换到另一个 Observable |
| `retry(n)` | 失败后重试 n 次（0 表示不重试） |
| `retry_when(f)` | 根据自定义逻辑决定何时重试 |

### 调度型（Scheduling）

| 方法 | 说明 |
|------|------|
| `observe_on(scheduler)` | 在指定调度器上执行下游的 on_next/on_completed |
| `subscribe_on(scheduler)` | 在指定调度器上调用源的 subscribe |

---

## Subject 实现

Subject 既是 Observable（可被订阅），又是 Observer（可调用 `on_next` / `on_completed`），因此常被用作**事件总线**。

### PublishSubject

```rust
use rx_rust::subject::PublishSubject;
use rx_rust::observer::FnObserver;

let subject = PublishSubject::<i32, ()>::new();

// 订阅者 A
subject.subscribe_ref(FnObserver::new(
    |v| println!("A: {:?}", v),
    || println!("A 完成"),
));

// 订阅者 B
subject.subscribe_ref(FnObserver::new(
    |v| println!("B: {:?}", v),
    || println!("B 完成"),
));

// 发射值（两个订阅者都会收到）
subject.on_next(Ok(1));  // A: Ok(1), B: Ok(1)
subject.on_next(Ok(2));  // A: Ok(2), B: Ok(2)
subject.on_completed();  // A 完成, B 完成
```

**特性**: 新订阅者只能收到订阅**之后**发射的值。

### BehaviorSubject

```rust
use rx_rust::subject::BehaviorSubject;

let subject = BehaviorSubject::<i32, ()>::new(42);  // 初始值 42

subject.subscribe_ref(FnObserver::new(
    |v| println!("S1: {:?}", v),
    || {},
));
// S1 立刻收到 Ok(42)

subject.on_next(Ok(100));
// S1 收到 Ok(100)

subject.subscribe_ref(FnObserver::new(
    |v| println!("S2: {:?}", v),
    || {},
));
// S2 立刻收到 Ok(100)（当前最新值）
```

**特性**: 每个新订阅者都会**立刻收到当前最新值**，适合表示"随时间变化的状态"。

### ReplaySubject

```rust
use rx_rust::subject::ReplaySubject;

let subject = ReplaySubject::<i32, ()>::new(3);  // 缓存最近 3 个值

subject.on_next(Ok(1));
subject.on_next(Ok(2));
subject.on_next(Ok(3));
subject.on_next(Ok(4));

subject.subscribe_ref(FnObserver::new(
    |v| println!("{:?}", v),
    || {},
));
// 立刻收到: Ok(2), Ok(3), Ok(4)（最近 3 个）
```

**特性**: 缓存并重放最近 N 个值给**每个新订阅者**。

---

## 调度器系统

`observe_on` 和 `subscribe_on` 允许你控制**哪部分代码在哪个线程上执行**。

### 可用的调度器

| 调度器 | 说明 |
|--------|------|
| `CurrentThreadScheduler` | 在当前线程同步执行（默认） |
| `ImmediateScheduler` | 立即执行，不做任何调度 |
| `ThreadPoolScheduler::new(n)` | 在 n 个 worker 线程池中执行 |
| `AsyncScheduler` | 异步调度执行 |

### observe_on：切换下游的执行线程

```rust
use rx_rust::prelude::*;
use rx_rust::scheduler::ThreadPoolScheduler;

let source = range::<i32, ()>(1, 3);

source
    .observe_on(ThreadPoolScheduler::new(2))
    .subscribe(FnObserver::new(
        |v| println!("在工作线程收到: {:?}", v),
        || println!("完成"),
    ));
```

### subscribe_on：切换订阅时的执行线程

```rust
source
    .subscribe_on(AsyncScheduler::new())
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));
```

---

## 错误处理

rx_rust 使用 `Result<T, E>` 作为值的传递类型：

```rust
use rx_rust::prelude::*;

let source = rx_rust::observable::base::throw::<i32, String>("网络失败".into());

source.subscribe(FnObserver::new(
    |v| match v {
        Ok(val) => println!("值: {}", val),
        Err(e)  => println!("错误: {}", e),
    },
    || println!("完成"),
));
```

### catch_error：从错误中恢复

```rust
// 失败后替换为备用的 Observable
let source = rx_rust::observable::base::throw::<i32, String>("失败".into());

source
    .catch_error(|_| rx_rust::observable::base::of::<i32, String>(-1))
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || println!("完成"),
    ));
// 输出: Ok(-1), 完成
```

### retry：自动重试

```rust
let call_count = std::sync::Arc::new(std::sync::Mutex::new(0));
let call_count_clone = call_count.clone();

let source = ObservableFn::<i32, String>::new(move |observer| {
    let mut count = call_count_clone.lock().unwrap();
    *count += 1;
    if *count <= 2 {
        observer.on_next(Ok(1));
        observer.on_next(Err("网络超时".into()));
    } else {
        observer.on_next(Ok(42));
        observer.on_completed();
    }
    Subscription::empty()
});

source.retry(3).subscribe(FnObserver::new(
    |v| println!("{:?}", v),
    || println!("完成"),
));
// 输出: Ok(1), Ok(1), Ok(42), 完成
```

---

## 示例代码

### 示例 1：简单的事件流

```rust
use rx_rust::prelude::*;

fn main() {
    // 模拟点击事件：过滤前 3 个，取偶数，平方，打印
    let clicks = from_iter::<i32, ()>(vec![1, 2, 3, 4, 5, 6, 7]);

    clicks
        .take(5)
        .filter(|x| x % 2 == 0)
        .map(|x| x * x)
        .subscribe(FnObserver::new(
            |v| println!("结果: {:?}", v),   // Ok(4), Ok(16)
            || println!("事件流结束"),
        ));
}
```

### 示例 2：搜索输入的防抖（debounce）

```rust
use std::time::Duration;
use rx_rust::prelude::*;

// 模拟用户在输入框打字
let subject = PublishSubject::<String, ()>::new();

subject
    .debounce(Duration::from_millis(300))      // 安静 300ms 才发射
    .filter(|s| !s.is_empty())                 // 过滤空字符串
    .subscribe(FnObserver::new(
        |s| println!("执行搜索: {:?}", s),
        || {},
    ));

// 用户快速输入
subject.on_next(Ok("h".into()));
subject.on_next(Ok("he".into()));
subject.on_next(Ok("hel".into()));
subject.on_next(Ok("hell".into()));
subject.on_next(Ok("hello".into()));
// 300ms 后打印: 执行搜索: Ok("hello")
```

### 示例 3：合并多个数据源

```rust
use rx_rust::prelude::*;

let keyboard_events = from_iter::<i32, ()>(vec![1, 3, 5]);
let mouse_events    = from_iter::<i32, ()>(vec![2, 4, 6]);

keyboard_events
    .merge(mouse_events)
    .subscribe(FnObserver::new(
        |v| println!("事件: {:?}", v),
        || println!("所有事件都处理完"),
    ));
```

### 示例 4：使用 Subject 做事件总线

```rust
use rx_rust::prelude::*;
use std::sync::{Arc, Mutex};

let bus = Arc::new(PublishSubject::<String, ()>::new());

// 模块 A 订阅
bus.clone().subscribe_ref(FnObserver::new(
    |msg| println!("[模块A] 收到: {:?}", msg),
    || {},
));

// 模块 B 订阅
bus.clone().subscribe_ref(FnObserver::new(
    |msg| println!("[模块B] 收到: {:?}", msg),
    || {},
));

// 任何地方都可以发射事件
bus.on_next(Ok("系统启动".into()));
bus.on_next(Ok("用户登录".into()));
bus.on_completed();
```

---

## 高级用法

### switch_map：切换到最新的 inner Observable

当你有一个"会产生新 Observable 的值"（如搜索框的每次输入都会触发一个网络请求），你只关心**最新的**那个结果：

```rust
use rx_rust::prelude::*;

let search_input = from_iter::<String, ()>(vec![
    "h".into(), "he".into(), "hel".into(), "hello".into()
]);

// 每次输入都切换到新的搜索请求（这里简化为一个 Observable）
search_input
    .switch_map(|query| {
        rx_rust::observable::base::of::<String, ()>(format!("搜索结果: {}", query))
    })
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));
```

### ConnectableObservable：多播 + 延迟启动

```rust
use rx_rust::prelude::*;

let hot = from_iter::<i32, ()>(vec![1, 2, 3]).publish();

// 先让多个订阅者订阅
hot.subscribe_ref(FnObserver::new(|v| println!("S1: {:?}", v), || {}));
hot.subscribe_ref(FnObserver::new(|v| println!("S2: {:?}", v), || {}));

// connect() 之后才真正开始发射（两个订阅者都能收到）
hot.connect();
```

---

## 常见问题

### Q1: Observable 是同步的还是异步的？

默认是**同步**的（订阅发生时，值就在当前线程被发射）。你可以用 `observe_on` / `subscribe_on` 配合 `ThreadPoolScheduler` 或 `AsyncScheduler` 使其变为异步。

### Q2: 为什么 `on_next` 接收 `Result<T, E>` 而不是分开的 `on_next(T)` / `on_error(E)`？

这是 rx_rust 的设计选择：`Result` 是 Rust 中处理值/错误的惯用法，所有值序列都可以用统一的模式匹配处理：

```rust
match value {
    Ok(v)  => /* 正常值 */,
    Err(e) => /* 错误 */,
}
```

### Q3: 如何创建自定义 Observable？

最方便的方式是用 `ObservableFn`：

```rust
use rx_rust::observable::{Observable, ObservableFn};
use rx_rust::subscription::Subscription;

let custom = ObservableFn::<i32, ()>::new(|observer| {
    for i in 1..=10 {
        observer.on_next(Ok(i));
    }
    observer.on_completed();
    Subscription::empty()
});
```

### Q4: 取消订阅后，源 Observable 会停止吗？

会。所有操作符内部都会检查 subscription 的 disposed 状态，一旦 `dispose()` 被调用，后续的值会被丢弃，源 Observable 也会被通知停止。

---

## 项目结构

```
rx-rust/
├── src/
│   ├── lib.rs                  # 库入口 + prelude 导出
│   ├── observable/
│   │   ├── mod.rs              # 模块声明
│   │   ├── observable.rs       # Observable trait + ObservableFn
│   │   └── base.rs             # 创建函数 (of, from_iter, range...)
│   ├── observer/
│   │   ├── mod.rs              # 模块声明
│   │   └── observer.rs         # Observer trait + FnObserver
│   ├── operators/
│   │   └── mod.rs              # 全部 40+ 操作符实现
│   ├── subject/
│   │   └── mod.rs              # Publish/Behavior/ReplaySubject + ConnectableObservable
│   ├── scheduler/
│   │   └── mod.rs              # 4 种调度器实现
│   └── subscription/
│       ├── mod.rs              # 模块声明
│       └── disposable.rs       # Subscription + Disposable trait
└── tests/
    ├── lib.rs                  # 集成测试入口
    └── integration/
        ├── observable/mod.rs   # 58 个 observable/subject 测试用例
        └── subscription/mod.rs # 4 个 subscription 测试用例
```

---

## 贡献

欢迎贡献！你可以：

1. **提交 Issue** - 报告 Bug 或请求新功能
2. **提交 PR** - 添加新操作符、优化现有实现、改进文档
3. **编写示例** - 丰富示例代码，帮助其他开发者

### 开发指南

```bash
# 运行测试
cargo test

# 运行特定测试
cargo test test_retry

# 检查类型
cargo check

# 检查警告
cargo clippy -- -D warnings
```

### 代码风格

- 遵循 [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- 4 空格缩进（不要 tabs）
- 所有公共 API 都应当有文档注释

---

## 许可证

**MIT License** © 2024 Rx-Rust Team

详见 [LICENSE](LICENSE) 文件。

---

## 版本历史

### 0.1.0 (2024)

- ✅ 初始版本发布
- ✅ 实现 Observable / Observer / Subscription 核心抽象
- ✅ 实现 40+ 操作符（创建、转换、过滤、数学、组合、时间、错误处理、调度）
- ✅ 实现 3 种 Subject（Publish / Behavior / Replay）
- ✅ 实现 4 种 Scheduler（CurrentThread / Immediate / ThreadPool / Async）
- ✅ 实现 ConnectableObservable（热 Observable）
- ✅ 实现 retry / retry_when 重试机制
- ✅ 62 个单元测试全部通过
- ✅ Python 绑定 (rxpy)

---

*最后更新：2024*
