# 构建与安装验证

验证日期：2026-10-11。以下构建、隔离及安装记录对应 **2026-10-11 首次验证基线**（产物 SHA-256 见 [功能与兼容性验证记录](VALIDATION.md)）；100 行默认配置产物的后续本地检查另列于该记录文末。构建环境为 OpenJDK 17、Maven 3.6.3；插件基线为 IoTDB Session 2.0.6、TsFile 2.2.0、官方 Apache JMeter 5.6.3。

## 构建结果

- `mvn package`：11 个单元测试通过，生成 `target/iotdb-jmeter-plugin.jar` 和 `target/iotdb-jmeter-plugin-distribution.zip`。
- 同一构建环境中连续两次执行 `mvn package`，插件 JAR 的 SHA-256 相同。构建插件和依赖版本均已固定，JAR 输入使用固定输出时间戳。
- 分发 ZIP 包含单个插件 JAR、安装脚本、JMX 示例、使用文档和结果汇总脚本。`original-iotdb-jmeter-plugin.jar` 是 Maven 的打包中间结果，请勿安装。

## 运行库隔离

JMeter 核心与 Java Sampler 依赖使用 `provided`，不打进插件。IoTDB、TsFile、Thrift、SLF4J 2、HTTP、Commons、ANTLR 和 XZ 类使用插件专用包名；SLF4J 的服务发现资源也已重定位。插件的公开采样器类名保持 `org.apache.iotdb.jmeter.test.JmeterTest`。

TsFile 2.2.0 已内嵌全部 27 个 `common` 类，且与独立 `common` JAR 的字节内容一致，因此分发包排除重复的 `common` 依赖。Snappy、Zstd 和 LZ4 保留原包名及原生库资源，满足 JNI 加载要求。

插件全部 `.class` 与官方 JMeter 5.6.3 标准分发包逐项比较，没有同名类冲突。此检查针对官方标准分发包；另外安装的第三方插件需另行确认其原生压缩库依赖。

## 安装与启动

Linux 安装脚本验证覆盖源码目录默认路径、解压后的分发包默认路径、含空格的目录、旧插件冲突拦截和无效 JMeter 目录。安装后的官方 JMeter 5.6.3 正常启动；使用不可连接端口运行 JMX 时产生一个 `SETUP_ERROR` 失败样本，确认写入行数和测点数均为 0，没有类加载或日志库链接错误。

PowerShell 安装脚本已提供，当前验证环境没有 PowerShell，未做 Windows 执行验证。上游保存的 `src/main/lib` 旧 JMeter JAR 不再参与编译或分发。

实际数据库写入和回读结果见 [功能与兼容性验证记录](VALIDATION.md)，部署步骤和正式测试范围见 [使用说明](USAGE.md)。
