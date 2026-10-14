# 固定点数写入方案

本方案只设置一个正式工况：10 个实际写入线程，各持有一个同步 IoTDB Session，并各自轮询 50 个不重叠逻辑源。每源固定写入 2400 批，每批 100 行，每行包含 `s0..s9` 十个 `INT64` 测点，共 500 源、120 万次正式请求、12 亿个唯一逻辑点。每源共 24 万行、240 万点，配额恰好整除，无不足 100 行的末批。

正式采样器为 `org.apache.iotdb.jmeter.test.FixedWorkloadSampler`，使用 [iotdb-fixed-workload.jmx](../examples/iotdb-fixed-workload.jmx)。原有 `JmeterTest` 和 [iotdb-write.jmx](../examples/iotdb-write.jmx) 保持通用写入行为；原有结果汇总器不承担本方案的准备阶段、源配额或正式验收判定。

## 配额与计时

- 每个线程仅有一个 Java Sampler 节点。首次调用用于独立预热和准备屏障，随后每次调用只提交一个单源、100 行的同步 Tablet；共执行 `1 + 50 × 2400 = 120001` 次迭代。源按 `0..49` 固定顺序轮询。
- 采样器先建好各源和预热设备的模式，并检查它们没有旧数据。首次调用为每个预热设备写入 100 行，所有 10 个线程准备成功后共同释放墙钟起点 `T0`。准备样本标记 `iotdb_phase=prepare`，确认行数、点数和尝试行数均为 0；预热数据不进入正式统计。
- 正式样本标记 `iotdb_phase=formal`。`T1` 是最后一个正式请求的终态时间；本版本不自动重试，出现准备或写入异常时停止整个测试并保留失败结果。未知或失败批次不直接算作成功确认点。
- 有效吞吐为成功确认且去重的正式点数除以 `(T1 − T0) / 1000` 秒。正式配额全部成功时，`≥ 20,000,000 点/秒` 等价于实际耗时 `≤ 60 秒`。60 秒是性能限值，计划关闭定时调度器，不在 60 秒强制停止。
- 每源从 `T0` 到完成配额期间的最长无成功确认（ACK）间隔须不超过 10 秒，包括首批等待。共同活跃区间取 `[max(各源首 ACK), min(各源完成 ACK)]`，长度须大于 0；已完成配额后的尾部不称为 500 源持续写入。固定点数结果不能外推为持续 60 秒或长期稳定性证据。

正式设备的默认路径为 `root.jmeter.fixed.run_<run_id>.worker_<n>.source_<k>`；预热设备使用同一线程路径下的 `warmup_<k>`。可用 `-Jdevice_path=<prefix>` 设置另一明确前缀；两次 JTL 审核均须相应设置 `--device-path <prefix>`。线程编号来自该运行内部注册，不能假定它等于 JMeter 的线程名称。每次运行必须提供新的 `run_id`，仅允许字母、数字和下划线；禁止重用包含旧数据的运行路径。

默认数据起始时间戳为 `1700000000000`，步长为 1 毫秒；第 `r` 行的时间戳为 `start_timestamp + r`，测点 `s<i>` 的值为 `r + i`。这是可复核的合成数据时间，不按数据时间戳限速。生成器可通过 `--start-timestamp` 固定另一明确起点；建模、连接、预热和数据库回读均不计入正式写入吞吐。

## 构建和生成

需要 JDK 17、Maven、JMeter 5.6.3，以及 Python 3.9+。受测系统使用毫秒时间精度和树模型 RPC；开测前确认服务器、客户端供数、网络与磁盘能力，固定版本、复制配置、请求超时、整轮运行上限及存储保护阈值。INT64 原始值约 9.6 GB，不含时间戳、WAL、副本和索引开销。

```bash
mvn clean verify
python3 -m unittest discover -s src/test/python
python3 scripts/generate_fixed_workload_plan.py \
  --output examples/iotdb-fixed-workload.jmx
export JMETER_HOME=/path/to/apache-jmeter-5.6.3
bash scripts/install-plugin.sh "$JMETER_HOME"
```

生成器把线程数、源数、批数和迭代数写为一致的固定值；正式计划不接受运行时 `-Jthreads`、`-Jloops`、`-Jduration`、`-Jop_count` 或 `-Jbatch_size` 改变配额。连接地址、对齐模式和超时通过计划中明确列出的属性设置。采样器没有单独的 `phase` 参数或 Groovy 控制器，自动将首次调用与后续正式调用分开。

## 运行正式计划

密码只从环境变量 `IOTDB_PASSWORD` 读取，不写入 JMX、命令行或结果文件。以下为 Bash 示例；先录入密码，再在同一终端启动 JMeter：

```bash
read -r -s -p 'IoTDB password: ' IOTDB_PASSWORD
printf '\n'
export IOTDB_PASSWORD
run_id="fixed_$(date +%Y%m%d_%H%M%S)_$$"
result_dir=$(mktemp -d)
sample_variables=iotdb_phase,iotdb_run_id,iotdb_formal_start_ms,iotdb_worker,iotdb_timestamp_is_start,iotdb_ack_rows,iotdb_ack_points,iotdb_attempted_rows,iotdb_device,iotdb_first_timestamp,iotdb_last_timestamp
"$JMETER_HOME/bin/jmeter" -n -t examples/iotdb-fixed-workload.jmx \
  -l "$result_dir/results.jtl" -j "$result_dir/jmeter.log" \
  -Jrun_id="$run_id" -Jhost=127.0.0.1 -Jport=6667 \
  -Jsampleresult.timestamp.start=true \
  -Jjmeter.save.saveservice.output_format=csv \
  -Jjmeter.save.saveservice.print_field_names=true \
  -Jjmeter.save.saveservice.timestamp_format=ms \
  -Jsample_variables="$sample_variables"
```

这些样本变量必须全部保存。JTL 的 `timeStamp` 为请求开始时间，`elapsed` 为毫秒耗时；采样器逐行输出 `iotdb_timestamp_is_start`，审核器要求每行均为 `true`。若没有启用开始时间记录或该字段缺失，审核器将拒绝数据，不猜测时间戳口径。`iotdb_formal_start_ms` 保存各线程共享的 `T0`。`iotdb_first_timestamp` / `iotdb_last_timestamp` 保存合成数据范围，两种时间用途不同。JMeter Throughput 仍是样本/秒，应使用正式确认点数另算有效吞吐。准备阶段默认最多等待 300000 毫秒；这不代表正式性能限值。

计划面向一个 JMeter 进程：准备屏障在该进程内协调 10 个线程。不要用 JMeter 远程分发运行此计划，多个进程的屏障和编号不能组成一个共同正式运行。

## JTL 审核和独立数据库回读

先生成确认账本清单与 JTL 审核 JSON，再用相同版本的插件独立回读数据库，最后绑定回读结果重新判定。以下命令使用默认本地地址、端口和用户名；连接其他端点时给回读命令传入与写入一致的 `--host`、`--port`、`--username`。若写入使用 `-Jaligned=true`，回读也须使用 `--aligned true`。

```bash
python3 scripts/validate_fixed_workload.py "$result_dir/results.jtl" \
  --run-id "$run_id" \
  --manifest-out "$result_dir/manifest.tsv" \
  --summary-out "$result_dir/jtl-audit.json"
java -cp target/iotdb-jmeter-plugin.jar \
  org.apache.iotdb.jmeter.test.FixedWorkloadReadback \
  --manifest "$result_dir/manifest.tsv" \
  --output "$result_dir/readback.json"
python3 scripts/validate_fixed_workload.py "$result_dir/results.jtl" \
  --run-id "$run_id" --readback "$result_dir/readback.json" \
  --manifest-out "$result_dir/manifest.tsv" \
  --summary-out "$result_dir/acceptance.json"
```

回读对清单中全部正式设备的十个测点检查模式、COUNT、首末时间戳，并抽查首、中、末行的值；结果绑定清单 SHA-256。它不扫描验证 12 亿个值的每一个值，也不证明 ACK 已完成持久化、复制排空或长期稳定性。服务器资源、排空与恢复等证据仍须另行采集。

第一步没有数据库回读时，满足 JTL 检查的正式运行状态为 `WRITE_COMPLETE_REQUIRES_READBACK`，不能报告通过。只有最终 JSON 的 `status=PASS` 且 `performance_acceptance=true`，并同时具备完整正式配额、吞吐限值、逐源间隔、共同活跃区间及绑定的数据库回读，才满足工具覆盖的本工况判据。`FAILED` 会返回非零退出码；查看 `errors`，保留原始 JTL、日志、清单及回读结果，不能修剪失败记录重新算吞吐。正式性能结果须在实际执行后填写；本说明和生成计划本身不是实测结果。

## 缩小工作量的功能验证

生成器只在显式 `--functional-only` 下允许减少线程、每线程源数或每源批数，仍保持每批 100 行、十个 INT64 测点。例如 2 线程 × 2 源 × 100 批写入 400000 个正式点：

```bash
python3 scripts/generate_fixed_workload_plan.py --functional-only \
  --workers 2 --sources-per-worker 2 --batches-per-source 100 \
  --output "$result_dir/functional.jmx"
```

用新的 `run_id` 和结果目录，按前述相同 JTL 保存参数运行此文件。两次 JTL 审核均须额外传入 `--functional-only --workers 2 --sources-per-worker 2 --batches-per-source 100`；中间的回读命令不变。完整回读并满足时序检查后，状态为 `FUNCTIONAL_VALIDATED`，`performance_acceptance` 仍为 `false`，不能据此声称达到 2000 万点/秒或 500 源正式验收。

短轮可能在其他源取得首个 ACK 前就完成部分源的配额，此时即使 COUNT 一致，共同活跃区间仍会判定失败。应保留失败记录，并在独立的新运行中增加批数验证；不能删除时序检查或改变固定 100 行的批量口径。

已有实际功能检查和失败记录见 [固定工作量工具验证](FIXED-WORKLOAD-VALIDATION.md)。其中没有正式性能通过结论。
