# IoTDB 2.x 写入兼容性验证

验证日期：2026-10-11（Asia/Shanghai）。**2026-10-11 首次验证基线**插件已在官方 IoTDB 2.0.6 和 2.0.8 上完成树模型普通写入、对齐写入及独立回读。以下真实服务端记录均对应首次验证基线 SHA-256；本日随后调整 100 行默认配置的产物及本地检查另列于文末，不继承这些实测结论。本记录是功能与兼容性验证，不是正式性能验收结果；尚未据此验证其他 2.x 版本或表模型写入。

## 环境与首次验证基线产物

| 项目 | 实际版本或设置 |
| --- | --- |
| IoTDB Session 客户端 | 2.0.6 |
| TsFile 客户端依赖 | 2.2.0 |
| IoTDB 服务端一 | 2.0.6，`SHOW VERSION` BuildInfo `c6ecbde` |
| IoTDB 服务端二 | 2.0.8，`SHOW VERSION` BuildInfo `b88b5dc` |
| JMeter | 官方 Apache JMeter 5.6.3 标准分发包 |
| 实际 Java 运行时 | OpenJDK 17.0.20.1，Linux amd64 |
| Maven | 3.6.3 |
| 服务端环境 | 两个独立的本地临时实例，独立数据目录；关闭自动创建 schema |
| 隔离 RPC 端口 | 2.0.6 使用 46667；2.0.8 使用 45667 |

首次验证基线 `iotdb-jmeter-plugin.jar` 的 SHA-256：

```text
6152aa54a21b23c271cabba36c3646b28cc167c0caf6f0189a1b7c729a28f370
```

首次验证时实际 JMeter 安装目录中的插件与此 SHA-256 一致，以下四组写入和鉴权失败检查均使用该产物。服务端采用 Apache 官方二进制分发包，并核对 SHA-512：2.0.6 的下载记录保存在下述 `v206-download-provenance.json`；2.0.8 对照已有 `/root/project/code/verification-work/runtime-provenance.json`。随后又通过实际连接执行 `SHOW VERSION` 核对服务端版本。

插件以 Java 8 字节码为编译目标，当前实际运行验证使用 Java 17。Windows 安装脚本已提供，当前环境未执行 Windows 安装测试。构建、依赖隔离、重复构建及安装检查见 [构建与安装验证](BUILD-VALIDATION.md)。

## 自动化检查

- 首次验证基线源码构建的 11 个 Java 单元测试全部通过。
- JTL 汇总脚本的 4 个 Python 检查全部通过，覆盖时间窗筛选、计数校验、重复输入和时间区间重叠。
- 首次验证基线源码在两个服务端上分别执行 `IoTDBWriteIT`：每个服务端均为 3 个测试通过，0 失败、0 错误、0 跳过。
- 集成测试覆盖全部七种类型的普通和对齐写入，5 行拆为 `2 + 2 + 1` 的三个批次；同一采样器连续调用两次，回读 10 行并逐项检查时间戳、值及 `IsAligned` 元数据。
- 预建 `DOUBLE` 测点后请求 `INT64` 写入，初始化阶段返回 `SETUP_ERROR`、停止线程，尝试行数和确认行数均为 0，独立查询确认没有写入数据。

真实服务端集成测试默认跳过。复现时为专用测试实例设置 `IOTDB_IT_HOST`、`IOTDB_IT_PORT`、`IOTDB_IT_USERNAME` 和 `IOTDB_PASSWORD`，再执行：

```bash
mvn -Pintegration verify
```

测试只清理自身随机生成设备的时间序列。权限、服务端配置和资源规模应符合实际测试环境要求。

## 真实 JMeter 写入与独立回读

使用仓库的 `examples/iotdb-write.jmx`，每组设置 2 个线程、每线程 2 次调用，每次 5 行、7 个测点，批次大小 2；`prepare_schema=true`，测点类型依次为 `BOOLEAN,INT32,INT64,FLOAT,DOUBLE,TEXT,STRING`。起始时间戳为 `1700000000000`，步长 1 毫秒；每线程使用独立设备，并为两次调用连续分配时间戳。

| 服务端 | 模式 | JMeter 样本 | 失败样本 | 确认行数 | 确认测点数 | 独立设备数 | 设备内重叠时间区间 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2.0.6 | 普通 | 4 | 0 | 20 | 140 | 2 | 0 |
| 2.0.6 | 对齐 | 4 | 0 | 20 | 140 | 2 | 0 |
| 2.0.8 | 普通 | 4 | 0 | 20 | 140 | 2 | 0 |
| 2.0.8 | 对齐 | 4 | 0 | 20 | 140 | 2 | 0 |

各组尝试行数等于确认行数，未确认行数为 0。JTL 使用样本开始时间以及 `iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp` 变量列，由 `scripts/summarize_results.py` 汇总。

JMeter 完成后，使用对应官方服务端分发包的 Session 库、独立 Java 进程回读所有 8 个设备。每个设备实际存储 10 行、70 个非空测点，时间戳精确为 `1700000000000..1700000000009`；七种值和对齐元数据全部符合预期。总计实际回读 80 行、560 个测点，与 JTL 确认计数一致。这些短任务的点速率不作为稳态吞吐量或性能通过证据。

## 失败行为与运行库

2.0.6 上分别运行缺失密码、随机错误密码的首次验证基线 JMeter 检查：原计划 1 个线程、3 次调用，实际均只产生 1 个 `SETUP_ERROR` 失败样本并停止该线程；尝试行数、确认行数和确认测点数均为 0。随机错误密码未出现在 JTL、JMeter 日志或启动输出中。不可连接端口的安装启动检查见 [构建与安装验证](BUILD-VALIDATION.md)。

写入 Session 显式设置 `maxRetryCount(0)` 和 `enableRedirection(false)`。另经 SDK 字节码检查，关闭默认重定向避免缓存 leader 写入异常后触发回退重发。此项是配置及代码检查结论；本次没有多节点故障注入验证。网络异常后的未确认批次仍需独立回读核账。

四组成功写入的 JMeter 日志没有异常、错误或类链接失败。首次验证基线插件单独加载时，Snappy、Zstd、LZ4 JNI 均完成 8192 字节压缩/解压一致性检查；原生库验证范围是上述 Linux amd64 环境。

## 本地证据位置

首次验证基线证据保留在临时目录中，未作为仓库交付文件提交；文件名中的 `final-` 仅指该次验证收尾产物：

| 目录 | 内容 |
| --- | --- |
| `/tmp/iotdb-jmeter-e2e-v206-85dab93p/` | 2.0.6 实例记录、最终集成测试、普通/对齐 JTL 和汇总、回读、鉴权失败证据 |
| `/tmp/iotdb-jmeter-e2e-xe1aadvt/` | 2.0.8 实例记录、最终集成测试、普通/对齐 JTL 和汇总、回读、最终原生库检查及验证运行脚本 |

每个服务端目录中的最终证据为：

- `final-artifact-sha256.txt`、`final-server-version.log`。
- `maven-integration-no-replay-final.log`、`final-failsafe-report.xml`。
- `final-normal.jtl`、`final-aligned.jtl`；对应的 `*-summary.json`、`*-jmeter.log`、`*-stdout.log`。
- `final-jtl-readback.log`、`final-readback-devices.json`。

2.0.6 目录另有 `final-missing-password.*`、`final-wrong-password.*` 及对应日志/汇总。2.0.8 目录另有 `v206-download-provenance.json`、`final-native-codec-smoke.log`、`final-smoke-run.log`、`final-smoke-results.json` 和独立回读源文件。较早的无 `final-` 前缀记录包含中间构建的检查，不代替上述最终证据。

两个临时实例均已停止，4 个记录进程均已退出、14 个记录端口均已关闭。清理记录见各目录的 `cleanup.json`；清理只针对本次创建且进程命令行匹配对应临时服务端目录的进程，证据文件保留。

## 100 行默认配置的本地校验

2026-10-11 后续调整仅将采样器 GUI、配置缺省值及示例 JMX 的 `op_count`、`batch_size` 统一为 `100`。仍由 `TabletFactory` 构造 Tablet，经 `SessionWriteClient` 调用 `insertTablet` 或 `insertAlignedTablet`，没有更改核心写入路径；通用测点默认仍为单个 `s0`。

本次 `iotdb-jmeter-plugin.jar` 的 SHA-256：

```text
577b8ca37b5d015f4aae1d5a3161c22bd3f9c40c6abbab7c5b6e00d094f62e4c
```

- `mvn clean verify`：12 个 Java 单元测试通过，0 失败、0 错误、0 跳过，完整插件和分发 ZIP 构建成功。
- 新增模拟客户端回归：10 个 `INT64` 测点、100 行默认配置，普通和对齐模式各连续执行两次；每样本仅提交一个 100 行 Tablet，响应计数为 100 确认行、1,000 确认点。每模式两次样本共 200 个时间戳互不重复，逐项核对全部生成值。
- `python3 -m unittest discover -s src/test/python -v`：4 个结果汇总检查通过。
- 示例 JMX 的默认值及分发 ZIP 内插件、JMX 与当前文件的一致性检查通过。

本次没有启动真实服务端、运行真实 JMeter 写入或执行数据库回读，也没有测量吞吐量。前文各组 140 点、7 种类型的历史实测及其基线 SHA-256 保留，不属于此 100 行默认产物的实测证据。

本地记录：`/tmp/iotdb-jmeter-tablet100-build.log`、`/tmp/iotdb-jmeter-tablet100-package-check.json`；Java 测试报告位于 `target/surefire-reports/`。当前可交付产物为 `target/iotdb-jmeter-plugin.jar` 和 `target/iotdb-jmeter-plugin-distribution.zip`。
