# Spark RDD 算子 → rx-rust 集成对比清单

> 对照 Apache Spark RDD 公开算子（Transformation + Action），评估哪些可以集成到 rx-rust 响应式流框架中。

---

## 一、图例

| 标记 | 含义 |
|------|------|
| ✅ | 已由 rx-rust 实现（Rust 核心或 Python 层） |
| 🟡 | 容易集成（已有基础件，少量开发量） |
| 🔵 | 需要开发（可行但与 RDD 语义有差异） |
| ❌ | 不适合（语义不兼容、无意义或重复） |

---

## 二、Transformation 算子（转换）

### map / flatMap / filter 类

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `map(func)` | `map` | ✅ | 完全等价 |
| `flatMap(func)` | `flat_map` | ✅ | 完全等价 |
| `filter(func)` | `filter` | ✅ | 完全等价 |
| `mapPartitions(func)` | — | ❌ | 分区概念是 Spark 特有的 |
| `mapPartitionsWithIndex(func)` | — | ❌ | 同上 |
| `mapValues(func)` | — | 🔵 | pair 型流的 map value，需要 Key-Value Observable 抽象 |

### 集合 / 分区操作

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `union(other)` | `merge` | ✅ | 两个流合并，交错发射 |
| `intersection(other)` | — | ❌ | 需要对有限数据集做集合运算，响应式流无界 |
| `distinct()` | `distinct` | ✅ | 已实现 |
| `distinct(numPartitions)` | `distinct` | ✅ | 忽略分区参数 |
| `cartesian(other)` | — | 🔵 | 笛卡尔积，合理但极少使用场景 |
| `zip(other)` | `zip` | ✅ | 完全等价 |
| `zipWithIndex()` | — | 🟡 | `Observable.zip(Observable.range(0, N), source)` |
| `zipWithUniqueId()` | — | 🟡 | 生成唯一 ID 后 zip |
| `zipPartitions()` | — | ❌ | 分区概念 |
| `coalesce(numPartitions)` | — | ❌ | 分区概念 |
| `repartition(numPartitions)` | — | ❌ | 分区概念 |

### 聚合类

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `reduce(func)` | `reduce` | ✅ | 完全等价 |
| `aggregate(zero)(seqOp, combOp)` | — | 🔵 | 需要区分 seq 和 comb 两阶段 |
| `fold(zero)(func)` | — | 🔵 | 带初始值的 reduce，类似 `scan` 但只取最终值 |
| `combineByKey` | — | 🔵 | Key-Value 分组聚合 |
| `reduceByKey(func)` | `group_by` + `reduce` | 🟡 | 可用现有算子组合 |
| `groupByKey()` | `group_by` | 🟡 | 已有 `group_by` |
| `aggregateByKey` | — | 🔵 | 同上，需要 KV 抽象 |
| `foldByKey` | — | 🔵 | 同上 |
| `sortBy(func)` | — | 🟡 | 对有限序列排序，`to_list().sort()` |
| `sortByKey()` | — | 🟡 | 同上 |
| `countByKey()` | — | 🟡 | `group_by` + `map` 取 count |
| `countByValue()` | — | 🟡 | `group_by` + `map` 取 count |

### 分区内排序类

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `glom()` | — | ❌ | 将每个分区的元素组成数组，无对应含义 |
| `pipe(command)` | — | ❌ | 外部进程管道，不适合 |
| `repartitionAndSortWithinPartitions` | — | ❌ | 分区概念 |

### Join 类

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `join(other)` | — | 🔵 | 内连接，KV 类型对相同 key 的 value 做连接 |
| `leftOuterJoin(other)` | — | 🔵 | 左外连接 |
| `rightOuterJoin(other)` | — | 🔵 | 右外连接 |
| `fullOuterJoin(other)` | — | 🔵 | 全外连接 |
| `cogroup(other)` | — | 🔵 | 按 key 分组两个 RDD |

### 其他转换

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `sample(withReplacement, fraction)` | — | 🟡 | 随机采样 |
| `randomSplit(weights)` | — | 🟡 | 按权重分流 |
| `subtract(other)` | — | ❌ | 集合差，需要有限集 |
| `subtractByKey(other)` | — | ❌ | 同上 |
| `cache()` / `persist()` | `share` / `publish` | 🟡 | 多播共享，语义相似 |
| `checkpoint()` | — | ❌ | 持久化概念 |
| `pipe(command)` | — | ❌ | 外部进程 |
| `keyBy(func)` | — | 🟡 | 提取 key 转为 KV 流 |

---

## 三、Action 算子（行动）

### 基础输出

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `collect()` | `to_list()` (subscribe 收集) | 🟡 | 收集所有元素 |
| `count()` | `count` | ✅ | 已实现 |
| `first()` | `first` | ✅ | 已实现 |
| `take(n)` | `take(n)` | ✅ | 已实现 |
| `takeSample(withReplacement, n)` | — | 🟡 | 带采样的 take |
| `takeOrdered(n)` | — | 🟡 | 排序后取前 n |
| `top(n)` | — | 🟡 | 取最大的 n 个 |
| `reduce(func)` | `reduce` | ✅ | 已实现（也是 Action） |
| `fold(zero)(func)` | — | 🔵 | 带初始值的 reduce |
| `aggregate(zero)(seqOp, combOp)` | — | 🔵 | 同上 |

### 输出到外部

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `saveAsTextFile(path)` | — | ❌ | 文件 IO 不是响应式核心关注 |
| `saveAsSequenceFile(path)` | — | ❌ | Hadoop 特定 |
| `saveAsObjectFile(path)` | — | ❌ | Java 序列化 |
| `saveAsHadoopFile(path)` | — | ❌ | Hadoop 特定 |
| `saveAsHadoopDataset` | — | ❌ | Hadoop 特定 |

### 查找 / 统计

| Spark RDD 算子 | 对应 rx-rust 算子 | 状态 | 说明 |
|----------------|-------------------|------|------|
| `countByKey()` | — | 🟡 | KV 统计 |
| `countByValue()` | — | 🟡 | 值统计 |
| `foreach(func)` | `do_on_next` / `subscribe(on_next=...)` | ✅ | 已实现 |
| `foreachPartition(func)` | — | ❌ | 分区概念 |
| `lookup(key)` | — | 🔵 | KV 查找 |
| `max()` | `max` | ✅ | 已实现 |
| `min()` | `min` | ✅ | 已实现 |
| `sum()` | `sum` | ✅ | 已实现 |
| `mean()` / `average()` | `average` | ✅ | 已实现 |
| `stdev()` | — | 🟡 | 标准差 |
| `variance()` | — | 🟡 | 方差 |
| `histogram(buckets)` | — | 🟡 | 直方图 |
| `isEmpty()` | — | 🟡 | 判空 |

---

## 四、汇总统计

| 分类 | 总数 | ✅ 已有 | 🟡 容易 | 🔵 需要开发 | ❌ 不适合 |
|------|------|--------|--------|------------|----------|
| Transformation | 38 | 10 | 9 | 11 | 8 |
| Action | 29 | 9 | 11 | 3 | 6 |
| **合计** | **67** | **19** | **20** | **14** | **14** |

---

## 五、优先集成建议（按投入产出比排序）

### Tier 1 — 立即可做（🟡 类）

| 算子 | 实现思路 | 代码量估计 |
|------|---------|-----------|
| `zipWithIndex` | `source.zip(Observable.range(0))` | 5 行 |
| `zipWithUniqueId` | 组合 `generate` + `zip` | 10 行 |
| `sortBy` | `to_list()` → `sorted()` → `from_iter` | 8 行 |
| `sample` | 按概率 filter 随机种子 | 6 行 |
| `randomSplit` | 分流到多个 Subject | 15 行 |
| `collect` / `to_list` | subscribe 收集到 Vec | 10 行 |
| `stdev` / `variance` | Welford 在线算法 | 20 行 |
| `isEmpty` | `take(1).count().map(c => c == 0)` | 5 行 |
| `cache` / `persist` | `share()` / `publish().ref_count()` | 已有基础 |
| `takeOrdered(n)` | 维护大小为 n 的堆 | 15 行 |
| `top(n)` | 同上，取最大的 n 个 | 15 行 |
| `histogram` | 等宽或等频分桶 | 15 行 |

### Tier 2 — 需要 KV 抽象基础（🔵 类，先做基础再做算子）

| 基础抽象 | 说明 |
|---------|------|
| `KeyValueObservable<K, V>` | 类似 Spark 的 `PairRDDFunctions` |
| 在此基础上可实现: | `mapValues`, `reduceByKey`, `groupByKey`, `countByKey`, `countByValue`, `join`, `leftOuterJoin`, `cogroup`, `lookup`, `keyBy` |

### Tier 3 — 语义不兼容（❌ 类，不建议）

- 分区相关 (mapPartitions, coalesce, repartition, glom)
- 外部存储 (saveAs*, checkpoint)
- 集合运算 (intersection, subtract, cartesian — 需要有限集)
- Hadoop 生态 (saveAsHadoop*, saveAsSequenceFile)

---

## 六、结论

1. rx-rust 已覆盖 **19/67 (28%)** 的 Spark RDD 算子，核心算子基本齐全
2. 另有 **20 个算子 (30%)** 可以极低成本集成（Tier 1），大多 5-20 行代码
3. **14 个算子 (21%)** 需要先建立 KV 抽象层（Tier 2），值得做但非紧急
4. 剩余 **14 个 (21%)** 不适合响应式流模型，主要涉及分区、文件 IO、集合运算
5. 建议优先实现 Tier 1 的 12 个算子，可快速将覆盖率提升到 **58% (39/67)**