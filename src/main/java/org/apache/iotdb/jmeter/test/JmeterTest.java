package org.apache.iotdb.jmeter.test;

import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.tsfile.write.record.Tablet;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/** IoTDB 2.x tree-model writer: one Session per JMeter sampler/thread. */
public class JmeterTest extends AbstractJavaSamplerClient {
  private static final Logger LOG = Logger.getLogger(JmeterTest.class.getName());
  private static final String AUTO_RUN_ID = UUID.randomUUID().toString().replace("-", "");
  private static final AtomicLong SAMPLER_IDS = new AtomicLong();
  private static final ConcurrentHashMap<String, AtomicLong> SHARED_TIMESTAMPS = new ConcurrentHashMap<>();

  private WriteConfig config;
  private WriteClient client;
  private TabletFactory tablets;
  private String device = "";
  private Exception setupFailure;
  private long nextTimestamp;
  private long nextValue;

  @Override
  public Arguments getDefaultParameters() {
    Arguments args = new Arguments();
    args.addArgument("host", "127.0.0.1");
    args.addArgument("port", "6667");
    args.addArgument("username", "root");
    args.addArgument("password", "");
    args.addArgument("device_path", "root.jmeter.device");
    args.addArgument("device_per_thread", "true");
    args.addArgument("run_id", "");
    args.addArgument("measurements", "s0");
    args.addArgument("data_types", "INT64");
    args.addArgument("op_count", "100");
    args.addArgument("batch_size", "100");
    args.addArgument("start_timestamp", "-1");
    args.addArgument("timestamp_step_ms", "1");
    args.addArgument("value_start", "0");
    args.addArgument("value_step", "1");
    args.addArgument("aligned", "false");
    args.addArgument("prepare_schema", "true");
    args.addArgument("stop_on_error", "true");
    args.addArgument("connection_timeout_ms", "5000");
    args.addArgument("query_timeout_ms", "60000");
    args.addArgument("rpc_compression", "false");
    return args;
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    closeClient();
    config = null;
    setupFailure = null;
    device = "";
    try {
      config = new WriteConfig(context);
      device = config.devicePath;
      if (config.devicePerThread) {
        String runId = config.runId.isEmpty() ? AUTO_RUN_ID : config.runId;
        device += ".run_" + runId + ".t" + SAMPLER_IDS.getAndIncrement();
      }
      nextTimestamp = config.startTimestamp == -1 ? System.currentTimeMillis() : config.startTimestamp;
      nextValue = config.valueStart;
      tablets = new TabletFactory(config, device);
      client = createClient();
      client.open(config);
      if (config.prepareSchema) {
        client.prepare(config, device);
      }
    } catch (Exception error) {
      setupFailure = error;
      closeClient();
    }
  }

  // Test seam stays inside the package; network-specific behavior is isolated.
  WriteClient createClient() {
    return new SessionWriteClient();
  }

  @Override
  public SampleResult runTest(JavaSamplerContext context) {
    SampleResult result = new SampleResult();
    result.setSampleLabel("IoTDB tree write");
    result.setDataType(SampleResult.TEXT);
    result.setContentType("application/json");
    result.setDataEncoding("UTF-8");
    result.setSamplerData(config == null ? "IoTDB setup" :
        config.host + ":" + config.port + "\n" + device + "\nrows=" + config.operationCount
            + ", columns=" + config.measurements.size() + ", batch_size=" + config.batchSize);
    int acknowledged = 0;
    int attempted = 0;
    int batches = 0;
    long first = -1;
    long last = -1;
    result.sampleStart();
    try {
      if (setupFailure != null) {
        throw setupFailure;
      }
      if (client == null || config == null) {
        throw new IllegalStateException("Sampler is not initialized; call setupTest first");
      }
      tablets.validateValues(nextValue, config.operationCount);
      long valueAfterSample = Math.addExact(nextValue,
          Math.multiplyExact(config.operationCount, config.valueStep));
      first = reserveTimestamps();
      last = Math.addExact(first, Math.multiplyExact(config.operationCount - 1L, config.timestampStep));
      long firstValue = nextValue;
      // Consume the whole range before submission. A failed RPC can have
      // committed some data; later samples must never replay that range.
      nextValue = valueAfterSample;
      for (int offset = 0; offset < config.operationCount;) {
        int rows = Math.min(config.batchSize, config.operationCount - offset);
        Tablet tablet = tablets.fill(rows, Math.addExact(first, Math.multiplyExact(offset, config.timestampStep)),
            Math.addExact(firstValue, Math.multiplyExact(offset, config.valueStep)));
        attempted += rows;
        batches++;
        client.insert(tablet, config.aligned);
        acknowledged += rows;
        offset += rows;
      }
      result.setSuccessful(true);
      result.setResponseCodeOK();
      result.setResponseMessage("Acknowledged " + acknowledged + " rows / "
          + ((long) acknowledged * config.measurements.size()) + " points");
    } catch (Exception error) {
      result.setSuccessful(false);
      result.setErrorCount(1);
      result.setResponseCode(setupFailure != null ? "SETUP_ERROR" : responseCode(error));
      result.setResponseMessage(safeMessage(error, context));
      result.setStopThread(setupFailure != null || config == null || config.stopOnError);
    } finally {
      result.sampleEnd();
    }
    // One invocation is one JMeter sample. Rows and scalar points are separate
    // counters, preserving correct request latency/error/throughput statistics.
    result.setSampleCount(1);
    int requested = config == null ? 0 : config.operationCount;
    long points = config == null ? 0 : (long) acknowledged * config.measurements.size();
    String json = "{\"requested_rows\":" + requested
        + ",\"attempted_rows\":" + attempted + ",\"acknowledged_rows\":" + acknowledged
        + ",\"acknowledged_points\":" + points + ",\"failed_rows\":" + (attempted - acknowledged)
        + ",\"unattempted_rows\":" + (requested - attempted) + ",\"batches\":" + batches
        + ",\"device\":\"" + device + "\",\"first_timestamp\":" + first
        + ",\"last_timestamp\":" + last + "}";
    result.setResponseData(json, "UTF-8");
    JMeterVariables variables = context.getJMeterVariables();
    if (variables != null) {
      variables.put("iotdb_ack_rows", Integer.toString(acknowledged));
      variables.put("iotdb_ack_points", Long.toString(points));
      variables.put("iotdb_attempted_rows", Integer.toString(attempted));
      variables.put("iotdb_device", device);
      variables.put("iotdb_first_timestamp", Long.toString(first));
      variables.put("iotdb_last_timestamp", Long.toString(last));
    }
    return result;
  }

  private long reserveTimestamps() {
    long span = Math.multiplyExact(config.operationCount, config.timestampStep);
    if (config.devicePerThread) {
      long first = nextTimestamp;
      nextTimestamp = Math.addExact(first, span);
      return first;
    }
    String key = config.host + ":" + config.port + "/" + device;
    AtomicLong clock = SHARED_TIMESTAMPS.computeIfAbsent(key, ignored -> new AtomicLong(nextTimestamp));
    while (true) {
      long current = clock.get();
      long first = Math.max(current, nextTimestamp);
      long next = Math.addExact(first, span);
      if (clock.compareAndSet(current, next)) {
        nextTimestamp = next;
        return first;
      }
    }
  }

  @Override
  public void teardownTest(JavaSamplerContext context) {
    closeClient();
  }

  private void closeClient() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception error) {
        // Avoid logging server exceptions that could contain credentials.
        LOG.warning("Unable to close an IoTDB Session (" + error.getClass().getSimpleName() + ")");
      } finally {
        client = null;
      }
    }
  }

  private static String responseCode(Exception error) {
    if (error instanceof StatementExecutionException) {
      int status = ((StatementExecutionException) error).getStatusCode();
      if (status > 0) {
        return Integer.toString(status);
      }
    }
    return error instanceof IllegalArgumentException || error instanceof ArithmeticException
        ? "CONFIG_ERROR" : "WRITE_ERROR";
  }

  private String safeMessage(Exception error, JavaSamplerContext context) {
    String message = error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage());
    String supplied = context.getParameter("password", "");
    if (!supplied.isEmpty()) {
      message = message.replace(supplied, "[redacted]");
    }
    String environmentPassword = System.getenv("IOTDB_PASSWORD");
    if (environmentPassword != null && !environmentPassword.isEmpty()) {
      message = message.replace(environmentPassword, "[redacted]");
    }
    return message.length() > 2000 ? message.substring(0, 2000) : message;
  }
}
