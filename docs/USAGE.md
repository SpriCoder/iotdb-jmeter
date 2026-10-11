# IoTDB JMeter 写入插件使用说明

## 适用范围与准备

本插件测试 IoTDB **树模型 Session RPC** 写入，使用 `insertTablet` 或 `insertAlignedTablet`。`device_path` 是 `root` 开头的设备路径；不支持把表模型的数据库名、表名、标签列或 SQL 端口作为本插件的写入配置。

推荐环境为 JDK 17、Maven 3.8+、Apache JMeter 5.6.3。代码以 Java 8 为编译目标，客户端依赖为 IoTDB Session 2.0.6，正式受测服务端固定为 Apache IoTDB 2.0.6。时间精度必须是 `ms`；插件在建立连接后检查该精度。确认 DataNode RPC 地址可达、测试账号有建模和写入权限，并为测试分配独立路径。

本插件只负责树模型 `insertTablet`/`insertAlignedTablet` 写入负载。它不提供跨 JMeter 进程的同步起跑、逐源每 10 秒连续性判定或最终数据库一致性结论；这些由 TC-01 的测试计划和独立核对脚本完成。历史 IoTDB 1.3.4 预检日志不得与 2.0.6 JMeter 结果合并。

## 构建与安装

从本项目目录构建并运行自动化检查：

```bash
mvn clean verify
```

构建生成包含运行时依赖的 `target/iotdb-jmeter-plugin.jar`，以及可分发的 `target/iotdb-jmeter-plugin-distribution.zip`。依赖使用隔离包名封装在插件内部，JMeter 自身依赖由 JMeter 提供，不需要额外复制 Maven 依赖 JAR。不要安装 `original-iotdb-jmeter-plugin.jar`，它不是包含依赖的最终插件。

### Linux / macOS

构建完成后，在项目目录执行：

```bash
export JMETER_HOME=/path/to/apache-jmeter-5.6.3
bash scripts/install-plugin.sh "$JMETER_HOME"
```

也可解压分发 ZIP，在解压后的 `iotdb-jmeter-plugin` 目录执行相同安装命令；脚本会自动读取包内的 `lib/ext/iotdb-jmeter-plugin.jar`。需要指定其他产物时，传入第二个参数：`bash scripts/install-plugin.sh "$JMETER_HOME" /path/to/iotdb-jmeter-plugin.jar`。

安装脚本检查 JMeter 目录并复制单个插件 JAR 到 `lib/ext`。发现其他命名的旧插件时会停止并提示；请先移除旧版 `jmeter-connector*.jar` 或重复插件，再运行脚本。若曾手动安装未封装的 IoTDB/TsFile 依赖，请清理确认属于旧插件的重复依赖，勿删除其他插件所需文件。安装后重启 JMeter。

插件在标准 JMeter 5.6.3 发行版中验证。原生压缩编解码库需要保留 JNI 类名，因此无法全部隔离包名；自定义 JMeter 或其他插件若携带不同版本的相同原生库，仍可能发生类加载或本地库冲突，需在实际部署组合中另行验证。

### Windows 手动安装

1. 安装 JDK、Maven 和 JMeter，确认 `java -version`、`mvn -version` 正常。
2. 在项目目录运行 `mvn clean verify`，或直接解压分发 ZIP。
3. 将 `target\iotdb-jmeter-plugin.jar`（源码构建）或 `lib\ext\iotdb-jmeter-plugin.jar`（分发包）复制到 JMeter 的 `lib\ext\`；不需要另行复制依赖。
4. 移除旧插件，重启 JMeter；用“Java 请求”的类名下拉框检查 `org.apache.iotdb.jmeter.test.JmeterTest` 是否可选。

Windows 也提供 PowerShell 安装工具，可在项目或解压目录执行：

```powershell
& .\scripts\install-plugin.ps1 -JMeterHome 'C:\tools\apache-jmeter-5.6.3'
```

如本机脚本执行策略不允许运行，使用上面的手动复制步骤。

正式压测使用非 GUI 模式。GUI 适合编辑计划和检查少量样本；不要在正式压测时开启“查看结果树”等大量保留响应的监听器。

## 密码与首次执行

插件的 `password` 默认留空；此时从 JMeter 进程的 `IOTDB_PASSWORD` 环境变量读取。显式指定 `password` 会覆盖环境变量。两者都未提供时，样本报告配置失败。

Linux/macOS Bash 可交互输入：

```bash
read -r -s -p 'IoTDB password: ' IOTDB_PASSWORD
printf '\n'
export IOTDB_PASSWORD
```

请从该终端启动 JMeter。图形桌面里已经运行的 JMeter 不会自动获取后来设置的环境变量。示例 JMX 不保存密码；把实际密码写入 `-Jpassword=...` 会使其出现在命令行中，通常优先使用环境变量。

### Linux / macOS 一次连通性测试

```bash
export JMETER_HOME=/path/to/apache-jmeter-5.6.3
result_dir=$(mktemp -d)
"$JMETER_HOME/bin/jmeter" -n -t examples/iotdb-write.jmx \
  -l "$result_dir/results.jtl" -j "$result_dir/jmeter.log" \
  -Jsampleresult.timestamp.start=true \
  -Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
```

默认一个线程、一个样本，每个样本用一个 Tablet 写入 100 行、一个测点。确认 JTL 的 `success` 为 `true`、`iotdb_ack_rows=100`、`iotdb_ack_points=100`，再增加负载。这是运行检查，不能作为性能验收证据。

### Windows PowerShell 一次连通性测试

```powershell
$iotdbSecurePassword = Read-Host 'IoTDB password' -AsSecureString
$env:IOTDB_PASSWORD = [System.Net.NetworkCredential]::new('', $iotdbSecurePassword).Password
$env:JMETER_HOME = 'C:\tools\apache-jmeter-5.6.3'
$resultDir = Join-Path $env:TEMP ('iotdb-jmeter-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $resultDir | Out-Null
& "$env:JMETER_HOME\bin\jmeter.bat" -n -t examples\iotdb-write.jmx `
  -l "$resultDir\results.jtl" -j "$resultDir\jmeter.log" `
  '-Jsampleresult.timestamp.start=true' `
  '-Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp'
```

## 计划与参数

示例计划为 [examples/iotdb-write.jmx](../examples/iotdb-write.jmx)，Java 请求类为 `org.apache.iotdb.jmeter.test.JmeterTest`。计划使用 `${__P(参数名,默认值)}` 读取 JMeter 属性，命令行用 `-J参数名=值` 覆盖。`-J` 属性不会自动覆盖手工填写固定值的其他 JMX。

### 线程控制参数

| JMeter 属性 | 默认值 | 作用 |
| --- | --- | --- |
| `threads` | `1` | JMeter 并发线程数，每线程持有独立 Session |
| `ramp_up` | `1` | 启动这些线程所用的秒数 |
| `loops` | `1` | 每线程样本数；`-1` 表示持续循环 |
| `duration` | `60` | 线程组运行时限，秒；持续测试通常同时设置 `loops=-1` |
| `delay` | `0` | 线程组启动延迟，秒 |

计划同时启用循环次数与调度时限，先达到的条件结束发起新样本。默认 `loops=1`，所以不会持续写满 60 秒。默认 `stop_on_error=true`，写入失败后停止该线程；配置或初始化失败始终停止该线程。到达时限不会精确截断正在执行的写入，因而实际结束时间可能晚于配置时限。

### 写入与连接参数

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `host` | `127.0.0.1` | DataNode 树模型 RPC 主机 |
| `port` | `6667` | RPC 端口，范围 1–65535 |
| `username` | `root` | IoTDB 用户名 |
| `password` | 空 | 显式密码；空时读取 `IOTDB_PASSWORD` |
| `device_path` | `root.jmeter.device` | 基础设备路径 |
| `device_per_thread` | `true` | 为采样器追加独立设备后缀 |
| `run_id` | 空 | 空时使用本 JVM 自动生成的 UUID 十六进制标识 |
| `measurements` | `s0` | 逗号分隔的测点名，顺序对应 `data_types` |
| `data_types` | `INT64` | 逗号分隔的数据类型，数量必须等于测点数量 |
| `op_count` | `100` | 每个 JMeter 样本计划写入的行数，正整数 |
| `batch_size` | `100` | 每个 Tablet 最多包含的行数，正整数 |
| `start_timestamp` | `-1` | 首个时间戳，毫秒；空或 `-1` 使用初始化时的墙钟时间 |
| `timestamp_step_ms` | `1` | 相邻行时间戳间隔，正整数，毫秒 |
| `value_start` | `0` | 生成值的起点 |
| `value_step` | `1` | 每推进一行的值增量 |
| `aligned` | `false` | 是否写入对齐序列 |
| `prepare_schema` | `true` | 在采样计时前检查/创建模式，验证类型和对齐方式 |
| `stop_on_error` | `true` | 写入失败后停止该线程；`false` 允许后续样本继续，初始化失败仍停止 |
| `connection_timeout_ms` | `5000` | 连接超时，毫秒 |
| `query_timeout_ms` | `60000` | 模式查询等查询的超时，毫秒；不表示整个测试的时限 |
| `rpc_compression` | `false` | 是否启用 RPC 压缩 |

旧计划中，`device_path` 缺省或为空时接受 `database` 别名；`measurements` 缺省或为空时接受单测点的 `measurement` 别名。`database` 在这里仍代表完整设备路径。新计划请使用 `device_path` 和 `measurements`。

设备路径的每个节点、测点名须为字母或下划线开头、后续只含字母数字下划线的简单标识符，不能含通配符；测点名不能重复。多个测点必须显式给出同样数量的数据类型，例如 `s0,s1` 对应 `INT64,INT64`，单个默认类型不会自动复制。

TC-01 使用 10 个 `INT64` 测点时，保持 `op_count=100`、`batch_size=100`，一次 JMeter 样本调用一次 Tablet 写入；成功确认 100 行，即 1,000 个点。具体的 500 源、共同 600 秒测量窗口及启动参数按报告执行说明配置。

### 设备与时间戳

`device_per_thread=true` 时，实际设备为：

```text
<device_path>.run_<run_id>.t<采样器唯一编号>
```

同一 JVM 的空 `run_id` 使用一个自动标识；采样器编号随实例增加，同一 JVM 中再次执行测试也会使用新的采样器设备。可以显式设置只含字母、数字和下划线的 `run_id`，便于按运行标识查询。本插件不会自动删除设备或数据。

时间戳在采样器间为相同设备协调预留，在样本开始写入前预留整个 `op_count` 行的区间。即使某批次失败，后续样本也使用后续区间，不重放失败批次、不重新使用已预留区间；数据可能出现时间戳空洞。指定过大的起点、步长或样本大小导致溢出时，会报告失败。

时间戳按生成行数推进，不按真实时间等待；`timestamp_step_ms=1` 只定义数据时间轴上的间隔，不会每写一行暂停 1 毫秒。写入足够快时，生成的数据时间戳可以超前于墙钟时间；负载速率由客户端、服务器和测试计划决定。

`device_per_thread=false` 会按原样写入 `device_path`。时间区间协调只对同一 JVM 中相同的 `host` 原文、`port` 和 `device_path` 组合生效；不会把主机名解析后视为同一个地址。主机名别名、不同端口或指向同一设备的多个接入端点不共享这一个时钟，独立进程也无法共享该状态。

不同进程、不同负载机和重复运行仍可能覆盖已有值；即使默认 `start_timestamp=-1` 使用墙钟起点，前次生成的时间戳超前于墙钟时也可能重叠，此时“确认测点数”可能大于新增测点数。默认的独立设备模式更适合吞吐量测试。显式指定 `run_id` 时，每次重复运行、每个分布式引擎都要使用新的独立标识；关闭独立设备后 `run_id` 不再修改设备路径，需直接为各次运行和引擎分配不同 `device_path`。各远程 JMeter 进程也须设置密码环境变量。

### 测点类型与生成值

支持 `BOOLEAN`、`INT32`、`INT64`、`FLOAT`、`DOUBLE`、`TEXT` 和 `STRING`，列表中的类型与测点按位置一一对应。数值基准为：

```text
value_start + 本采样器已分配的行序号 × value_step + 测点列序号
```

行序号从 0 开始并跨样本推进，列序号从 0 开始。数值列使用该值；`BOOLEAN` 在该值为偶数时取 `true`；`TEXT`/`STRING` 使用 UTF-8 字符串 `v<数值>`。每一行的全部测点都有值，因此成功确认的一行贡献“测点数量”个点。设置的值必须适合目标数据类型。

`FLOAT` 和 `DOUBLE` 由整数序列转换；绝对值很大时可能失去整数精度，相邻序号不一定得到不同的浮点值。需要按值核对序号时，优先使用 `INT64` 或文本序列，或把生成值控制在目标类型可精确表示的范围内。

`prepare_schema=true` 时，各线程的连接建立、时间精度检查、已有模式检查和必要的建模发生在 `setupTest` 中，不进入写入样本耗时。测点类型和已有模式必须一致；对齐设备不能按普通设备写入，反之亦然。关闭此参数时，请先自行准备正确模式，或确认服务端自动建模设置；否则初次写入可能触发建模开销或失败。

## 多测点与持续压测

两线程各执行两次，每样本 100 行、两测点，每 50 行一个 Tablet：

```bash
"$JMETER_HOME/bin/jmeter" -n -t examples/iotdb-write.jmx \
  -l "$result_dir/multi.jtl" -j "$result_dir/multi.log" \
  -Jthreads=2 -Jloops=2 -Jop_count=100 -Jbatch_size=50 \
  -Jmeasurements=s0,s1 -Jdata_types=INT64,DOUBLE \
  -Jsampleresult.timestamp.start=true \
  -Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
```

全部成功时：4 个 JMeter 样本，8 个 Tablet，400 行，800 个测点。对齐写入增加 `-Jaligned=true`，并使用未被普通序列占用的新设备路径或新的运行标识。

持续压测示例，先独立预热 60 秒，再单独测量 300 秒。两次运行使用不同设备标识和不同 JTL：

```bash
"$JMETER_HOME/bin/jmeter" -n -t examples/iotdb-write.jmx \
  -l "$result_dir/warmup.jtl" -j "$result_dir/warmup.log" \
  -Jthreads=8 -Jramp_up=10 -Jloops=-1 -Jduration=60 \
  -Jrun_id=warmup_demo_001 \
  -Jsampleresult.timestamp.start=true \
  -Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
"$JMETER_HOME/bin/jmeter" -n -t examples/iotdb-write.jmx \
  -l "$result_dir/measure.jtl" -j "$result_dir/measure.log" \
  -Jthreads=8 -Jramp_up=10 -Jloops=-1 -Jduration=300 \
  -Jrun_id=measure_demo_001 \
  -Jsampleresult.timestamp.start=true \
  -Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
```

再次运行前更换示例 `run_id`。独立预热可以预热服务端，但正式运行会新建客户端连接。上述 300 秒包含正式线程组的爬升时间；若需要稳态口径，另行选择所有目标线程启动后的测量窗口。记录预热、爬升、采样窗口和整个命令的实际耗时，不能把预热数据或建模耗时混入未经说明的吞吐量指标。

JMeter HTML 报告可由测量阶段的 JTL 生成：

```bash
"$JMETER_HOME/bin/jmeter" -g "$result_dir/measure.jtl" -o "$result_dir/html" \
  -Jsampleresult.timestamp.start=true \
  -Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
```

HTML 报告中的 Throughput 仍是样本/秒，不能直接作为测点/秒。

## 响应、JTL 和统计口径

每次 `runTest` 只计一个样本。样本计时包含 Tablet 构造、各批次写入调用和相关处理；不含正常的连接建立与预建模。SDK 客户端的自动重试、leader 重定向和缓存路由均关闭，请求固定发往配置端点；发生结果不明确的写入失败时，SDK 不会重放该请求，插件停止本样本剩余批次，并将失败批次保留为未确认行。RPC 超时或网络断开并不能证明服务端未写入。

响应为 JSON，适合在 GUI 中检查少量样本或在失败时诊断：

| JSON 字段 | 含义 |
| --- | --- |
| `requested_rows` | 此样本计划写入的行数 |
| `attempted_rows` | 已发起写入调用的行数，包含失败批次 |
| `acknowledged_rows` | 收到成功确认的行数 |
| `acknowledged_points` | 确认行数 × 测点数量 |
| `failed_rows` | 发起但未获得成功确认的行数；并不保证服务端没有写入 |
| `unattempted_rows` | 失败后未再尝试的行数 |
| `batches` | 已发起的 Tablet 写入调用数，包含失败的最后一次调用 |
| `device` | 实际设备路径 |
| `first_timestamp` / `last_timestamp` | 此样本预留的首末时间戳，毫秒 |

首末时间戳表示整段预留区间，包含失败批次和没有发起的尾部行，不代表已确认写入的区间。配置或初始化失败时，时间边界可能没有有效值。默认 CSV JTL 通常不保留响应正文，需在启动前设置 `sample_variables` 保存以下列。示例所有运行命令已包含该设置：

```text
iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
```

`sample_variables` 是 JMeter 启动属性，需在 GUI 启动命令、非 GUI 运行命令或 `user.properties` 中设置，仅在计划里添加同名用户变量无效。保留 CSV 格式和表头、毫秒 epoch 时间戳；统计需要 `timeStamp`、`elapsed`、`success`、`label` 等标准列。所有示例明确设置 `-Jsampleresult.timestamp.start=true`，保证 `timeStamp` 表示样本开始，结束时间才能按 `timeStamp + elapsed` 计算；汇总工具无法从 JTL 自动推断该属性。

设测量窗口为 `[T0,T1]`，单位秒。完整运行时可用最早样本开始作为 `T0`，最晚样本结束作为 `T1`；稳态分析则另选边界，明确如何处理跨边界样本。

```text
样本/秒 = 纳入窗口的 JMeter 样本数 / (T1 - T0)
确认行/秒 = Σ iotdb_ack_rows / (T1 - T0)
确认测点/秒 = Σ iotdb_ack_points / (T1 - T0)
失败样本比例 = success=false 的样本数 / 全部样本数
```

分子与分母必须使用同一批样本和同一窗口。并发线程的 `elapsed` 有重叠，不能把各样本耗时直接相加当作墙钟测量时长。失败样本中的部分确认写入也应纳入确认行和确认测点；把失败样本整行删除会少计已确认数据。也不能用 `samples × op_count × 测点数` 替代实际确认计数。

结果至少按测试阶段、运行标识/来源、采样器标签和相同配置分组。不要直接合并预热与测量、不同批量大小、不同测点数量或不同引擎的 JTL；汇总多引擎时校准时钟，并使用共同墙钟窗口。保留各组的样本数、确认行/点、失败率、延迟分位数和时长，说明实际设备模式与线程数。

### 用随附工具汇总 JTL

需要 Python 3。对一个配置一致的测量 JTL 执行：

```bash
python3 scripts/summarize_results.py "$result_dir/measure.jtl" > "$result_dir/summary.json"
```

工具输出 JSON，包含 `samples`、`failed_samples`、`acknowledged_rows`、`acknowledged_points`、`unconfirmed_rows`、`acknowledged_points_per_second`、墙钟窗口以及各设备计数。默认窗口使用最早样本开始到最晚样本结束，失败样本已经确认的行/点也计入分子；所有样本的时长为零时，吞吐量为 `null`。

稳态分析可显式指定毫秒 epoch 窗口。以下数字仅示例，请替换为自己的实际采样边界：

```bash
python3 scripts/summarize_results.py "$result_dir/measure.jtl" \
  --window-start-ms 1791680400000 --window-end-ms 1791680700000 \
  > "$result_dir/steady-summary.json"
```

两个边界必须同时提供。指定窗口后，工具只纳入开始和结束都位于窗口内的完整样本，跨边界样本计入 `excluded_samples`；分母使用指定窗口的完整时长。该方式不会将跨边界样本中的单个批次按比例拆分。

工具可接受多个 JTL 路径并合并，合并前应确认相同负载配置、相同测试阶段和一致的统计窗口。工具不会自动识别预热阶段、负载配置或采样器标签并分组，需先按来源选择文件或筛选，再分别运行。其 `possible_overlapping_timestamp_ranges` 用于提示同设备样本区间可能重叠，不能据此证明当前数据库没有已有数据。`count_reconciliation_required` 始终为 `true`，仍需按下面方法核验数据库计数；任何失败都要保留未确认行并单独调查。

## 用数据库计数核验

先做可精确预期的小规模测试，使用独立且未使用过的设备。例如单线程写 3 个样本，每样本 100 行、两测点：

```bash
"$JMETER_HOME/bin/jmeter" -n -t examples/iotdb-write.jmx \
  -l "$result_dir/verify.jtl" -j "$result_dir/verify.log" \
  -Jthreads=1 -Jloops=3 -Jdevice_per_thread=false \
  -Jdevice_path=root.jmeter.verify.run_demo_001 \
  -Jop_count=100 -Jbatch_size=40 \
  -Jmeasurements=s0,s1 -Jdata_types=INT64,DOUBLE \
  -Jsampleresult.timestamp.start=true \
  -Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
```

全部成功时，JTL 应有 3 行样本，累计确认 300 行、600 个测点。在已连接树模型的 IoTDB SQL CLI 中运行：

```sql
SELECT count(s0), count(s1) FROM root.jmeter.verify.run_demo_001;
SELECT s0, s1 FROM root.jmeter.verify.run_demo_001 ORDER BY TIME LIMIT 5;
```

两列 `count` 应各为 300，其和为 600；查看数据还可核验类型、值序列和时间间隔。每次重复测试都更换设备路径，避免已有数据影响比较。

对默认独立设备模式，按本次 `run_id` 查询所有采样器设备，例如：

```sql
SELECT count(s0) FROM root.jmeter.device.run_measure_demo_001.* ALIGN BY DEVICE;
```

将所有设备的 `count(s0)` 相加，与本次 JTL 的确认行数之和比较。多测点时查询每个实际测点，再累计所有设备所有测点的计数。自定义 `device_path` 或测点名时相应修改 SQL。

如果使用了未使用过的设备、没有失败且全部测点非空，数据库总计数应等于确认测点总数。出现失败时，超时批次可能已经部分或全部落库，数据库计数可能高于客户端确认计数。保留失败响应与日志并核对对应的时间区间；不要自动重试相同区间来“修正”统计。清理测试数据是独立操作，本插件没有自动删除逻辑。

## 自动化检查与真实服务端验证

在源码项目目录运行单元测试和汇总工具检查：

```bash
mvn test
python3 -m unittest discover -s src/test/python -v
```

Java 单元测试覆盖配置、生成值、批次边界、失败后的确认计数和时间戳行为，不需要数据库。Python 检查使用标准库，覆盖汇总窗口和计数等逻辑。分发 ZIP 不包含测试源码，运行这些检查应使用源码项目。

在运行 Maven 的终端设置 `IOTDB_PASSWORD` 后，可运行需要真实 IoTDB 的集成检查：

```bash
mvn -Pintegration verify -Dit.host=127.0.0.1 -Dit.port=6667
```

`it.host` 也可通过 `IOTDB_IT_HOST` 设置；`it.port` 对应 `IOTDB_IT_PORT`；测试用户名默认 `root`，可用 `-Dit.username=...` 或 `IOTDB_IT_USERNAME` 修改。未提供主机时，真实服务端检查会跳过，不能把“构建成功”当作“数据库验证成功”。

集成检查创建独立的 `root.jmeter_it.test_<UUID>` 设备，验证各数据类型、批次尾部、重复采样、对齐方式和数据库读回，并在结束时删除本次测试时序；账号需具备相应建模、写入、查询和删除测试时序的权限。该清理属于集成测试，本插件运行示例计划时仍不会自动删除数据。最终部署还应以 JMeter 非 GUI 运行示例计划并独立执行 SQL 计数，验证打包和类路径。

自动化测试、真实服务端的小规模检查都验证功能，不提供正式负载的性能验收结论。实际验证环境、最终产物标识、各项检查和独立回读证据，见 [功能与兼容性验证记录](VALIDATION.md)。

## 常见问题

| 现象 | 检查方向 |
| --- | --- |
| Java 请求类不可选或 `ClassNotFoundException` | 最终 `iotdb-jmeter-plugin.jar` 是否在 `lib/ext`，是否误用了不含依赖的 `original-*.jar`，是否重启 JMeter |
| `NoSuchMethodError` / `NoClassDefFoundError` | JMeter 类路径是否混有旧 IoTDB、TsFile、Thrift 或 SLF4J 依赖；检查 `jmeter.log` |
| 原生压缩库/JNI 加载错误 | 自定义 JMeter 或其他插件是否加载了不同版本的相同原生编解码库；在标准发行版或实际插件组合中核验 |
| 密码缺失或登录失败 | 环境变量是否设置在真正启动 JMeter 的进程中，显式密码/用户名是否正确 |
| 连接超时/拒绝连接 | 主机、RPC 端口、网络、DataNode 状态以及连接超时设置 |
| 时间精度失败 | 服务端须为 `timestamp_precision=ms`，本插件不会替服务端修改配置 |
| 模式/对齐冲突 | 更换独立设备，或先准备与 `data_types`、`aligned` 一致的模式 |
| JTL 没有 `iotdb_*` 列 | 在启动前设置 `sample_variables`，保留 CSV 表头；检查自定义 JMeter 保存配置 |
| JMeter 吞吐量与预期测点/秒不符 | 检查是否把样本/秒当成测点/秒，是否存在失败、覆盖或混合统计窗口 |
| 运行时间长于 `duration` | 时限停止发起新样本，当前调用和清理可能继续；查询超时也不是写入样本总时限 |

正式结果需同时保留计划、非敏感启动参数、JTL、日志、数据库计数及软硬件配置。自动化检查和小规模连通性测试用于验证功能；它们不替代规定负载、规定时长下的验收测试。
