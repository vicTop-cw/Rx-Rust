# Polars DataFrame → rx-rust 算子对照 & 最终扩展清单

> Polars 是 Rust 编写的高性能列式 DataFrame 库，核心设计理念与 rx-rust 都基于 Rust，但处理范式不同（批处理 vs 响应式流）。
> 本文按 Polars 算子分类逐一对照，输出**最终需要扩展或完善的算子清单**。

---

## 一、rx-rust 已有算子基线（对照基准）

### 1.1 Rust 核心层（`rx-rust/src/operators/mod.rs`）

| 分类 | 已有算子 |
|------|---------|
| 转换 | `map`, `flat_map`, `scan`, `switch_map` |
| 过滤 | `filter`, `take`, `skip`, `first`, `last`, `take_while`, `skip_while`, `distinct_until_changed`, `distinct`, `contains`, `all`, `ignore_elements` |
| 组合 | `merge`, `zip`, `concat`, `combine_latest` |
| 聚合 | `reduce`, `count`, `sum`, `average`, `min`, `max` |
| 时间 | `debounce`, `throttle`, `timeout` |
| 错误处理 | `catch_error`, `retry`, `retry_when` |
| 工具 | `take_last`, `skip_last`, `element_at`, `default_if_empty`, `publish`, `buffer`, `collect` |
| 工厂 | `of`, `from_iter`, `empty`, `never`, `range`, `repeat` |

**合计：35 个算子**

### 1.2 Python 绑定层（`rx-rust-py/python/rx_rust/__init__.py`）

| 分类 | 已有算子 |
|------|---------|
| 转换 | `map`, `filter`, `flat_map`, `scan`, `start_with`, `do_on_next` |
| 过滤 | `take`, `skip`, `first`, `last`, `contains`, `all`, `default_if_empty` |
| 聚合 | `reduce`, `count`, `sum` |
| 组合 | `merge`, `concat` |
| 时间 | `delay`, `debounce`, `throttle`, `timeout`, `interval`, `timer` |
| 主题 | `PublishSubject`, `BehaviorSubject`, `ReplaySubject` |
| 调度器 | `CurrentThreadScheduler`, `ThreadPoolScheduler`, `AsyncScheduler`, `ImmediateScheduler` |
| 工厂 | `of(*values)`, `from_iter`, `range`, `repeat`, `empty`, `never` |
| 工具 | `collect`, `run`, `pipe`, `Subscription`, `CompositeSubscription` |

**Python 层核心算子约 28 个，但 Rust 层的 `min/max/average/zip/combine_latest/switch_map/distinct/drop_*` 未完全暴露到 Python。**

---

## 二、Polars 算子分类对照

### 2.1 基础转换（Transformation）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `select(expr)` | ⚠️ 无对应概念 | 不适用（DataFrame 列选择，rx-rust 是单一流） | P5 |
| `with_columns(expr)` | ⚠️ 无对应概念 | 不适用 | P5 |
| `drop(cols)` | ⚠️ 无对应概念 | 不适用 | P5 |
| `rename(mapping)` | ⚠️ 无对应概念 | 不适用 | P5 |
| `cast(dtype)` | ✅ `map(lambda x: new_type(x))` | 已有 map 实现，但缺少显式类型安全 cast | P3 |
| `filter(predicate)` | ✅ `filter` | 完全等价，已实现 | — |
| `drop_nulls()` | ⚠️ `filter(lambda x: x is not None)` | **建议新增** `drop_none` | P2 |
| `drop_duplicates()` | ✅ `distinct` | Rust 层已有，**建议 Python 暴露** | P2 |
| `fill_null(value/strategy)` | 🔴 缺失 | **建议新增** `default_if_empty` 扩展为 `fill_none` | P2 |
| `unique()` | ✅ `distinct` | 已存在 | — |

### 2.2 算术运算（Arithmetic）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `add/sub/mul/div` | ✅ `map(lambda x: x + 1)` | 通过 map 实现，无需新增 | — |
| `mod/pow` | ✅ `map(lambda x: x % n)` | 同上 | — |
| `abs` | 🔴 缺失 | **建议新增** `abs()` | P2 |
| `sqrt/exp/log/log10/log2` | 🔴 缺失 | **建议新增** `sqrt()`, `exp()`, `log()` | P2 |
| `ceil/floor/round` | 🔴 缺失 | **建议新增** `round(n)` | P3 |
| `clamp(min, max)` | 🔴 缺失 | **建议新增** `clamp(min, max)` | P2 |

### 2.3 字符串操作（String）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `str.contains` | ✅ `filter + str.__contains__` | 可通过组合实现 | — |
| `str.replace` | ✅ `map(lambda x: x.replace(...))` | 可通过组合实现 | — |
| `str.starts_with/ends_with` | ✅ `filter + str.startswith` | 可通过组合实现 | — |
| `str.split` | 🔴 缺失 | **建议新增** `split(sep)` / `flatten()` 组合 | P3 |
| `str.slice` | ✅ `map(lambda x: x[i:j])` | 可通过组合实现 | — |
| `str.strip/upper/lower` | ✅ `map` | 可通过组合实现 | — |
| `str.len_bytes / n_chars` | ✅ `map(len)` | 可通过组合实现 | — |
| `str.count_matches` | ✅ `map + re.findall` | 可通过组合实现 | — |

### 2.4 聚合运算（Aggregation）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `min` | ✅ Rust 层已有，**Python 缺失** | **立即补充** Python 绑定 | P1 |
| `max` | ✅ Rust 层已有，**Python 缺失** | **立即补充** Python 绑定 | P1 |
| `mean` | ✅ `average` | Rust 已有，**Python 缺失** | P1 |
| `median` | 🔴 缺失 | **建议新增** `median()` | P2 |
| `sum` | ✅ `sum` | 已实现 | — |
| `count` | ✅ `count` | 已实现 | — |
| `variance` | 🔴 缺失 | **建议新增** `variance()` | P2 |
| `std` | 🔴 缺失 | **建议新增** `std()` | P2 |
| `first` | ✅ `first` | 已实现 | — |
| `last` | ✅ `last` | 已实现 | — |
| `quantile(0.95)` | 🔴 缺失 | **建议新增** `quantile(q)` | P2 |
| `arg_min / arg_max` | 🔴 缺失 | **建议新增** `arg_min()`, `arg_max()` | P2 |
| `approx_n_unique` | 🔴 缺失 | 使用 HyperLogLog，**建议新增** | P4 |
| `n_unique` | 🔴 缺失 | **建议新增** `n_unique()` | P2 |

### 2.5 分组操作（Group By）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `group_by(key)` | 🔴 缺失 | **建议新增** `group_by(key_fn)` → 发射按 key 分组的流 | P1 |
| `agg(funcs)` | 🔴 缺失 | `group_by` 后应用聚合子算子 | P1 |
| `pivot` | ⚠️ 概念不匹配 | DataFrame 的行列转换，响应式流不适用 | P5 |
| `group_by_dynamic` | ✅ 可通过 `window` 模拟 | `group_by(time_window)` 已有基础 | P3 |
| `group_by_rolling` | ✅ 可通过 `rolling_*` 模拟 | 建议新增 `rolling` 系列 | P2 |
| `over(window)` | 🔴 缺失 | **建议新增** `window(partition_fn, order_fn, op)` | P3 |

### 2.6 连接操作（Join）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `join(inner)` | 🔴 缺失 | **建议新增** `join(other, key_fn)` 按 key 匹配 | P3 |
| `left_join` | 🔴 缺失 | 同上，需 Key-Value Observable 基础 | P3 |
| `right_join` | 🔴 缺失 | 同上 | P3 |
| `outer_join` | 🔴 缺失 | 同上 | P3 |
| `anti_join` | 🔴 缺失 | 同上 | P4 |
| `semi_join` | 🔴 缺失 | 同上 | P4 |
| `cross_join` | ⚠️ 笛卡尔积 | 响应式流不适合无限笛卡尔积 | P5 |

### 2.7 排序操作（Sorting）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `sort(key, descending)` | 🔴 缺失 | **建议新增** `sort(key_fn, reverse=False)` — **需有限流** | P1 |
| `sort_by(keys)` | 🔴 缺失 | 同上，多 key 排序 | P1 |
| `arg_sort` | 🔴 缺失 | **建议新增** `arg_sort(key_fn)` | P2 |
| `top_k(k)` | 🔴 缺失 | **建议新增** `top_k(k, key_fn)` — 维护大小为 k 的堆 | P1 |
| `bottom_k(k)` | 🔴 缺失 | **建议新增** `bottom_k(k, key_fn)` | P1 |

### 2.8 窗口/滚动运算（Window/Rolling）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `rolling_min(window)` | 🔴 缺失 | **建议新增** `rolling_min(window_size)` | P1 |
| `rolling_max(window)` | 🔴 缺失 | **建议新增** `rolling_max(window_size)` | P1 |
| `rolling_mean(window)` | 🔴 缺失 | **建议新增** `rolling_mean(window_size)` | P1 |
| `rolling_sum(window)` | 🔴 缺失 | **建议新增** `rolling_sum(window_size)` | P1 |
| `rolling_std(window)` | 🔴 缺失 | **建议新增** `rolling_std(window_size)` | P2 |
| `rolling_var(window)` | 🔴 缺失 | **建议新增** `rolling_var(window_size)` | P2 |
| `rolling_median(window)` | 🔴 缺失 | **建议新增** `rolling_median(window_size)` | P2 |
| `rolling_quantile(window, q)` | 🔴 缺失 | **建议新增** `rolling_quantile(window_size, q)` | P2 |

### 2.9 形变操作（Reshape）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `melt` | ⚠️ 不适用 | DataFrame 宽转长，流概念不匹配 | P5 |
| `pivot` | ⚠️ 不适用 | 同上 | P5 |
| `transpose` | ⚠️ 不适用 | 同上 | P5 |
| `unnest` | 🔴 缺失 | **建议新增** `flatten()` 用于嵌套 Observable | P2 |
| `explode` | 🔴 缺失 | **建议新增** `explode()` 发射 Iterable 的每个元素 | P1 |

### 2.10 时间操作（Temporal）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `dt.year/month/day/hour/...` | ✅ `map(lambda x: x.year)` | 可通过 map 实现 | — |
| `dt.strftime` | ✅ `map(lambda x: x.strftime(...))` | 可通过 map 实现 | — |
| `dt.strptime` | ✅ `map` | 可通过 map 实现 | — |
| 时间窗口 | ⚠️ `buffer(count)` 已有基础 | 建议扩展为 `buffer_by_time(seconds)` | P2 |

### 2.11 布尔/比较（Boolean/Comparison）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `eq/neq/lt/gt/lte/gte` | ✅ `filter(lambda x: x > n)` | 可通过 filter/map 实现 | — |
| `is_between(a, b)` | ✅ `filter(lambda x: a <= x <= b)` | 可通过组合实现 | — |
| `is_null / is_not_null` | ⚠️ `filter + None check` | **建议新增** `is_none()`, `filter_none()` | P2 |
| `is_finite / is_nan` | 🔴 缺失 | **建议新增** `filter_finite()`, `filter_nan()` | P3 |
| `any / all` | ✅ `all(predicate)` | `all` 已实现，**建议新增** `any(predicate)` | P2 |

### 2.12 列表/数组操作（List）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `list.get(idx)` | 🔴 缺失 | **建议新增** `element_at(idx)` — Rust 层已有但**Python 缺失** | P1 |
| `list.slice(a, b)` | 🔴 缺失 | **建议新增** `slice(start, end)` | P2 |
| `list.len` | ✅ `map(len)` | 可通过 map 实现 | — |
| `list.concat` | ✅ `merge / flat_map` | 可通过组合实现 | — |
| `list.contains` | ✅ `filter + in` | 可通过组合实现 | — |
| `list.first / list.last` | ✅ `map(lambda x: x[0]/[-1])` | 可通过组合实现 | — |
| `list.min/max/sum/mean` | ✅ `map + min/max/sum/average` | 可通过组合实现 | — |
| `list.sort` | 🔴 缺失 | **建议新增** `sort_inner(key_fn)` | P3 |
| `list.reverse` | ✅ `map(lambda x: x[::-1])` | 可通过组合实现 | — |

### 2.13 累积操作（Cumulative）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `cum_sum` | ✅ `scan(0, lambda acc, x: acc + x)` | 已可实现，**建议新增** `cum_sum()` 快捷方式 | P1 |
| `cum_min` | 🔴 缺失 | **建议新增** `cum_min()` | P1 |
| `cum_max` | 🔴 缺失 | **建议新增** `cum_max()` | P1 |
| `cum_mean` | 🔴 缺失 | **建议新增** `cum_mean()` | P2 |
| `cum_prod` | 🔴 缺失 | **建议新增** `cum_prod()` | P2 |

### 2.14 集合操作（Set）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `horizontal_concat` | 🔴 缺失 | **建议新增** `zip_with(other, combiner)` | P2 |
| `vertical_concat` | ✅ `concat / merge` | 已存在 | — |
| `align_frames` | ⚠️ 不适用 | DataFrame 对齐，不适用 | P5 |

### 2.15 采样（Sampling）

| Polars 算子 | rx-rust 现状 | 建议 | 优先级 |
|------------|--------------|------|--------|
| `sample(n, fraction, with_replacement)` | 🔴 缺失 | **建议新增** `sample(fraction)` | P2 |
| `shuffle` | 🔴 缺失 | **建议新增** `shuffle()` — 需缓冲全部 | P3 |

---

## 三、最终需要扩展的算子清单（按优先级排序）

### 🏆 P1 — 立即扩展（核心竞争力缺口）

| # | 算子名 | 位置 | 说明 | 预计代码行数 |
|---|--------|------|------|------------|
| 1 | `min()` | Python 层 | Rust 已有，补 Python 绑定 | 5 |
| 2 | `max()` | Python 层 | Rust 已有，补 Python 绑定 | 5 |
| 3 | `mean()` / `average()` | Python 层 | Rust 已有，补 Python 绑定 | 5 |
| 4 | `group_by(key_fn)` | Rust + Python | 按 key 分流，发射 (key, Observable) | 60 |
| 5 | `sort(key_fn, reverse)` | Python 层 | 缓冲 + 排序后发射（有限流） | 15 |
| 6 | `top_k(k, key_fn)` | Python 层 | 维护大小为 k 的堆，高效取前 k | 20 |
| 7 | `bottom_k(k, key_fn)` | Python 层 | 同上，取最小的 k | 15 |
| 8 | `explode()` | Python 层 | Iterable 展开为独立元素 | 10 |
| 9 | `element_at(idx)` | Python 层 | Rust 已有，补 Python 绑定 | 5 |
| 10 | `cum_sum()` | Python 层 | `scan(0, lambda acc, x: acc+x)` 快捷方式 | 8 |
| 11 | `cum_min()` | Python 层 | 累积最小值 | 8 |
| 12 | `cum_max()` | Python 层 | 累积最大值 | 8 |

**P1 小计：12 个算子，约 170 行代码**

---

### 🥇 P2 — 尽快扩展（常用统计 / 数据处理）

| # | 算子名 | 位置 | 说明 | 预计代码行数 |
|---|--------|------|------|------------|
| 13 | `distinct()` | Python 层 | Rust 已有，补 Python 绑定 | 5 |
| 14 | `drop_none()` | Python 层 | 过滤掉 None 值 | 8 |
| 15 | `fill_none(default_value)` | Python 层 | 将 None 替换为默认值 | 10 |
| 16 | `abs()` | Python 层 | 绝对值 | 6 |
| 17 | `sqrt()` / `exp()` / `log()` | Python 层 | 数学函数快捷 | 12 |
| 18 | `clamp(min_val, max_val)` | Python 层 | 将值夹在区间内 | 8 |
| 19 | `median()` | Python 层 | 中位数计算（需缓冲） | 15 |
| 20 | `variance()` | Python 层 | 方差（在线算法） | 15 |
| 21 | `std()` | Python 层 | 标准差（在线算法） | 15 |
| 22 | `quantile(q)` | Python 层 | 分位数计算 | 15 |
| 23 | `arg_min()` / `arg_max()` | Python 层 | 返回极值的下标 | 20 |
| 24 | `n_unique()` | Python 层 | 不重复值计数（set 缓冲） | 10 |
| 25 | `any(predicate)` | Python 层 | 任一值满足谓词则发射 True | 15 |
| 26 | `is_none()` / `filter_none()` | Python 层 | None 值检测与过滤 | 12 |
| 27 | `sample(fraction)` | Python 层 | 按概率随机采样 | 10 |
| 28 | `rolling_min(window)` | Python 层 | 滚动窗口最小值 | 15 |
| 29 | `rolling_max(window)` | Python 层 | 滚动窗口最大值 | 15 |
| 30 | `rolling_sum(window)` | Python 层 | 滚动窗口求和 | 15 |
| 31 | `rolling_mean(window)` | Python 层 | 滚动窗口平均 | 15 |
| 32 | `buffer_by_time(seconds)` | Python 层 | 基于时间的缓冲分组 | 15 |
| 33 | `flatten()` | Python 层 | 嵌套 Observable 展开 | 12 |
| 34 | `slice(start, end)` | Python 层 | 切片发射 | 10 |
| 35 | `cum_mean()` | Python 层 | 累积平均 | 10 |
| 36 | `cum_prod()` | Python 层 | 累积乘积 | 8 |

**P2 小计：24 个算子，约 312 行代码**

---

### 🥈 P3 — 中期完善（增强功能 / 类型安全）

| # | 算子名 | 位置 | 说明 | 预计代码行数 |
|---|--------|------|------|------------|
| 37 | `round(n)` | Python 层 | 四舍五入到 n 位小数 | 6 |
| 38 | `filter_finite()` / `filter_nan()` | Python 层 | 浮点数特殊值过滤 | 10 |
| 39 | `zip_with(other, combiner)` | Python 层 | 合并两个流并用自定义函数组合 | 15 |
| 40 | `switch_map` | Python 层 | Rust 已有，补 Python 绑定 | 8 |
| 41 | `combine_latest` | Python 层 | Rust 已有，补 Python 绑定 | 10 |
| 42 | `take_last(n)` | Python 层 | Rust 已有，补 Python 绑定 | 8 |
| 43 | `skip_last(n)` | Python 层 | Rust 已有，补 Python 绑定 | 8 |
| 44 | `take_while(predicate)` | Python 层 | Rust 已有，补 Python 绑定 | 8 |
| 45 | `skip_while(predicate)` | Python 层 | Rust 已有，补 Python 绑定 | 8 |
| 46 | `ignore_elements()` | Python 层 | Rust 已有，补 Python 绑定 | 5 |
| 47 | `catch_error(handler)` | Python 层 | Rust 已有，补 Python 绑定 | 10 |
| 48 | `retry(n)` / `retry_when(notifier)` | Python 层 | Rust 已有，补 Python 绑定 | 15 |
| 49 | `sort_inner(key_fn)` | Python 层 | 对 Iterable 元素内部排序 | 12 |
| 50 | `window(partition_fn, op)` | Python 层 | 分区窗口运算基础 | 40 |
| 51 | `distinct_until_changed()` | Python 层 | Rust 已有，补 Python 绑定 | 5 |
| 52 | `publish / share()` | Python 层 | 多播共享 Observable | 25 |
| 53 | `shuffle()` | Python 层 | Fisher-Yates 洗牌（需全部缓冲） | 15 |
| 54 | `join(other, key_fn)` | Rust + Python | 内连接，需 KV 基础 | 50 |
| 55 | `rolling_std/var/median/quantile` | Python 层 | 高级滚动统计 | 40 |

**P3 小计：19 个算子，约 303 行代码**

---

### 🥉 P4 — 高级扩展（专业统计 / 性能优化）

| # | 算子名 | 说明 | 预计代码行数 |
|---|--------|------|------------|
| 56 | `approx_n_unique()` | HyperLogLog 近似不重复计数 | 50 |
| 57 | `left_join / right_join(other, key_fn)` | KV 外连接 | 60 |
| 58 | `outer_join(other, key_fn)` | KV 全外连接 | 40 |
| 59 | `anti_join(other, key_fn)` | 反连接 | 30 |

**P4 小计：4 个算子，约 180 行代码**

---

### 🚫 P5 — 不适用（DataFrame 概念不适合响应式流）

- `select / with_columns / drop / rename` — 列操作概念
- `melt / pivot / transpose` — DataFrame 行列转换
- `cross_join` — 无限笛卡尔积不安全
- `cast(dtype)` — Rust 已类型安全，Python 动态语言无需显式 cast
- `horizontal_concat / align_frames` — 列对齐概念
- 分区分片相关操作

---

## 四、整体统计

| 优先级 | 数量 | 累计预计代码行数 | 对 rx-rust 能力提升 |
|--------|------|-----------------|---------------------|
| P1 | 12 | 170 | 核心数据处理能力，对标 Polars 80% 常用算子 |
| P2 | 24 | 312 | 完整统计 + 滚动窗口 + 采样能力 |
| P3 | 19 | 303 | 完善 Rust→Python 绑定 + 高级组合算子 |
| P4 | 4 | 180 | 专业统计与 KV Join |
| P5 | 6 | — | 不适用 |

**扩展后 rx-rust Python 层算子总数：28（已有） + 59（P1-P4） = 87 个**
**扩展后 rx-rust Rust/Python 两层覆盖率 ≈ 85% 的 Polars 单值 / 序列流处理能力**

---

## 五、实现路线建议

### Phase 1 — 低挂果（1-2 天完成 P1 中仅需绑定的算子）
- `min`, `max`, `mean`, `element_at`, `distinct`, `sort`, `top_k`, `bottom_k`
- 全部可以在 `_PyObservable` 中用已有基础快速实现
- 工作量：约 80 行代码

### Phase 2 — 组合增强（1-2 天完成 P1 剩余 + P2 基础）
- `group_by`, `explode`, `cum_sum`, `cum_min`, `cum_max`
- `drop_none`, `fill_none`, `abs`, `clamp`, `any`
- 工作量：约 120 行代码

### Phase 3 — 统计补全（2-3 天完成 P2 统计类）
- `median`, `variance`, `std`, `quantile`, `arg_min`, `arg_max`, `n_unique`
- `rolling_min/max/sum/mean`, `buffer_by_time`
- 工作量：约 180 行代码

### Phase 4 — 绑定完善（3-4 天完成 P3 的 Rust→Python 暴露）
- 将 Rust 层 `switch_map`, `combine_latest`, `take_while`, `skip_while`,
  `take_last`, `skip_last`, `catch_error`, `retry`, `retry_when`,
  `ignore_elements`, `distinct_until_changed`, `publish/share` 暴露到 Python
- 工作量：约 120 行代码 + 测试

### Phase 5 — 高级功能（3-5 天完成 P3 剩余 + P4）
- KV Observable 基础架构 → `group_by().agg()`
- Join 系列操作
- HyperLogLog 近似计数
- 工作量：约 250 行代码 + 测试

---

## 六、核心技术观察

1. **Polars 的强大之处**：列式内存布局 + SIMD 向量化 + 惰性执行计划优化。
   rx-rust 目前是元素级的操作符链式，**不具备列式/向量化优化**，
   因此数值计算场景性能仍落后 Polars 1-2 数量级。

2. **rx-rust 的优势**：在响应式 / 流处理场景（无限数据源、时间敏感），
   rx-rust 的 `debounce`, `throttle`, `timeout`, `interval`, `Subject`
   等时间感知算子是 Polars **天然缺失**的。

3. **互补建议**：
   - 有限数据集处理 → Polars
   - 无限流 / 事件驱动 → rx-rust
   - 两者组合 → `rx-rust` 做流处理窗口 → `Polars` 做窗口内数据分析
