# Rx-Rust 开发工具

本目录包含开发过程中使用的工具和配置。

## 子目录

| 目录 | 描述 |
|------|------|
| `benchmarks/` | 性能基准测试脚本和报告 |
| `configs/` | 项目配置文件 |

## benchmarks/

| 文件 | 描述 |
|------|------|
| `benchmark.py` | 基准测试脚本 |
| `benchmark_v2.py` | 基准测试脚本 v2 |
| `benchmark_report.md` | 基准测试报告 |
| `benchmark_report_v2.md` | 基准测试报告 v2 |

## configs/

| 文件 | 描述 |
|------|------|
| `rustfmt.toml` | Rust 代码格式化配置 |

## 运行基准测试

```bash
cd dev-tools/benchmarks
python benchmark.py
```