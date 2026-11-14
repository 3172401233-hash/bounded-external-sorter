# bounded-external-sorter
Python 有界内存外部排序库，支持多键稳定排序、分块落盘、归并和失败清理

## 用法

```python
from bounded_external_sorter import ExternalSorter

sorter = ExternalSorter(
    8 * 1024 * 1024,          # 显式内存预算（字节）
    keys=["age", "name"],     # 多个排序键，全部升序，从左到右生效
    nulls="last",             # 或 "first"：null / 缺失键排在最前或最后
    temp_dir="/var/tmp",      # 可选，spill 文件的父目录
)
result = sorter.sort(open("input.jsonl", "rb"))

result.records    # 排序后的原始行（bytes），稳定有序
result.rejected   # 无法解析/类型冲突的行，保留原始字节和原因
result.runs       # 每轮 spill 的元数据：记录数与起止序号
```

排序规则：

- 键值类型为 int / float / string / null；int 与 float 属于同一数值列，可混排。
- 数值列与字符串列不能混排：某键列的第一个非 null 值确定列类型，之后出现
  类型冲突的记录进入 `rejected`。bool、数组、对象等不支持的类型同样进入
  `rejected`，非有限浮点数（NaN/Infinity）也会被拒绝。
- 排序稳定：键完全相同时由原始记录序号（行号，从 0 开始）决定先后。
  每个 spill 文件头部记录该轮的起止序号，k 路归并以 `(键, 序号)` 为堆序，
  因此归并不会破坏稳定性；同一输入在任意预算下输出顺序完全一致。

## 内存预算如何换算

预算单位是**字节**，只约束内存中的记录缓冲区，估算公式：

```
每条记录占用 ≈ len(原始行字节) + RECORD_OVERHEAD_BYTES   # 常量 128
缓冲上限     ≈ Σ 每条记录占用 ≤ memory_budget
```

即 `memory_budget ≈ (平均行长 + 128) × 希望每批缓存的记录数`。
例如平均行长 200 字节、希望每批约 1 万条：
`(200 + 128) × 10000 ≈ 3.3 MB`，预算设为 `4 << 20` 即可。

边界条件：

- 预算低于 `MIN_MEMORY_BUDGET_BYTES`（512 字节）时构造即抛
  `InsufficientMemoryError`。
- 单条记录估算占用超过预算时抛 `InsufficientMemoryError`（该记录永远无法
  入缓冲）。
- 超出预算即把当前缓冲稳定排序后落盘为一个 spill run；归并阶段按堆逐条
  流式读取，不再占用与输入规模成正比的内存。

## 失败与取消语义

- `TempFileWriteError`：spill 目录/文件创建或写入失败（如磁盘满、权限不足），
  已产生的临时文件全部清理。
- `MergeInputError`：k 路归并输入少于 2 个 run 时抛出。
- `SortCancelledError`：传入的 `cancel_event`（`threading.Event`）被置位时，
  读取、落盘、归并任一阶段都会尽快中止。
- 无论成功、失败还是取消，每次排序的私有临时目录都会在 `finally` 中删除，
  不会留下可见的临时结果。

## 运行测试

```bash
python -m unittest
```

测试用最小预算（512 字节）强制多次落盘，覆盖多键排序、null 顺序、坏行、
重复键稳定性、临时写失败清理、归并确定性，以及不同预算下输出一致。
