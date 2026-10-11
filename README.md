# IoTDB JMeter 写入插件

使用 JMeter Java Sampler 对 **IoTDB 2.0.6 树模型**进行批量写入测试。客户端固定使用 IoTDB Session 2.0.6 和 TsFile Tablet API，支持多测点、对齐序列、线程独立设备，以及写入行数和测点数核验。

本插件是 TC-01 的写入负载发生器，不是完整验收判定器。正式验收仍须由测试计划固定 500 个不重叠源、共同测量窗口和唯一运行标识，并独立执行数据库 COUNT、首末值抽样、重复/丢失核对及服务端资源采集。JMeter Throughput 是样本/秒，不能直接当作唯一测点/秒。

建议使用 JDK 17、Maven 3.8+、JMeter 5.6.3；代码以 Java 8 为目标。IoTDB 服务端必须采用毫秒时间精度 `timestamp_precision=ms`，并开放树模型 RPC 接口，通常为 `6667`。

## 构建与安装

在本项目目录执行：

```bash
mvn clean verify
export JMETER_HOME=/path/to/apache-jmeter-5.6.3
bash scripts/install-plugin.sh "$JMETER_HOME"
```

产物 `target/iotdb-jmeter-plugin.jar` 是包含运行时依赖的独立插件包，安装到 JMeter 的 `lib/ext` 后重启即可。`target/iotdb-jmeter-plugin-distribution.zip` 同时包含插件、示例、说明和安装工具。安装细节见 [完整使用说明](docs/USAGE.md#构建与安装)。

## 首次运行

先安装插件、启动 IoTDB，并在启动 JMeter 的同一个终端中设置密码。Linux/macOS Bash 示例：

```bash
read -r -s -p 'IoTDB password: ' IOTDB_PASSWORD
printf '\n'
export IOTDB_PASSWORD
```

设置自己的 JMeter 安装路径，然后运行示例计划。输出放在临时目录：

```bash
export JMETER_HOME=/path/to/apache-jmeter-5.6.3
result_dir=$(mktemp -d)
"$JMETER_HOME/bin/jmeter" -n -t examples/iotdb-write.jmx \
  -l "$result_dir/results.jtl" -j "$result_dir/jmeter.log" \
  -Jsampleresult.timestamp.start=true \
  -Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
```

默认连接 `127.0.0.1:6667`，用户名 `root`；密码从 `IOTDB_PASSWORD` 读取，示例不含密码。默认每线程一个样本，每个样本用一个 Tablet 写入 100 行、一个 `INT64` 测点，因此本次运行预期确认 100 个测点。设备默认追加本次进程的运行标识和采样器编号，避免默认多线程互相覆盖。Windows 的安装和 PowerShell 运行方式见 [完整使用说明](docs/USAGE.md)。

用随附工具汇总实际确认的行数、测点数、失败样本与墙钟吞吐量（需要 Python 3）：

```bash
python3 scripts/summarize_results.py "$result_dir/results.jtl" > "$result_dir/summary.json"
```

通过 `-J参数名=值` 调整计划，例如：

```bash
"$JMETER_HOME/bin/jmeter" -n -t examples/iotdb-write.jmx \
  -l "$result_dir/multi.jtl" -j "$result_dir/multi.log" \
  -Jthreads=2 -Jloops=2 -Jop_count=100 -Jbatch_size=50 \
  -Jmeasurements=s0,s1 -Jdata_types=INT64,DOUBLE \
  -Jsampleresult.timestamp.start=true \
  -Jsample_variables=iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
```

此例全部成功时共 4 个 JMeter 样本、400 行、800 个测点。持续测试可设置 `-Jloops=-1 -Jduration=300`；该时长包含线程爬升，实际结束时间还可能包含正在执行的批次和连接清理。

## 如何读结果

- 一个 `runTest` 调用始终是 **一个 JMeter 样本**，其中可以包含多个 Tablet 批次。JMeter 的 Throughput 是样本/秒。
- `iotdb_ack_rows` 是已收到成功确认的行数，`iotdb_ack_points` 是这些行对应的测点数。测点/秒应由确认测点数和选定的墙钟测量窗口计算。
- 失败样本也可能已经确认了部分批次；其确认行数仍应保留。超时批次的实际落库情况需要数据库查询核验。
- 连接、时间精度检查和默认模式下的建模检查在采样计时前完成；样本耗时包含 Tablet 构造和写入调用。失败时不会复用已预留的时间戳，也不会在后续样本中重放失败批次。

完整参数表、独立预热、SQL 计数核验、结果统计口径、故障排查和分布式运行限制，见 [docs/USAGE.md](docs/USAGE.md)。实际验证版本、独立回读结果和功能验证范围，见 [docs/VALIDATION.md](docs/VALIDATION.md)。
