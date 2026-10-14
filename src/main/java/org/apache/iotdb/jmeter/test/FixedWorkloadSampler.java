package org.apache.iotdb.jmeter.test;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;

/** Fixed 100-row batches, round-robin sources, one synchronous Session per worker. */
public class FixedWorkloadSampler extends AbstractJavaSamplerClient {
  private WriteConfig config;
  private WriteClient client;
  private FixedRunCoordinator run;
  private TabletFactory[] tablets;
  private String workerPath = "";
  private int worker = -1;
  private int sources;
  private int batchesPerSource;
  private long totalCalls;
  private long submitted;
  private long prepareTimeoutMs;
  private long startMillis;
  private boolean prepared;
  private Exception setupFailure;

  @Override
  public Arguments getDefaultParameters() {
    Arguments args = new JmeterTest().getDefaultParameters();
    args.removeArgument("device_per_thread");
    args.removeArgument("stop_on_error");
    replace(args, "device_path", "root.jmeter.fixed");
    replace(args, "measurements", "s0,s1,s2,s3,s4,s5,s6,s7,s8,s9");
    replace(args, "data_types", "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64");
    replace(args, "start_timestamp", "1700000000000");
    args.addArgument("workers", "10");
    args.addArgument("sources_per_worker", "50");
    args.addArgument("batches_per_source", "2400");
    args.addArgument("prepare_timeout_ms", "300000");
    return args;
  }

  private static void replace(Arguments args, String name, String value) {
    args.removeArgument(name);
    args.addArgument(name, value);
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    // JMeter invokes setup lazily. Preparation and warm-up precede the shared T0.
    try {
      config = new WriteConfig(context);
      require(!config.runId.isEmpty(), "Fixed workload requires a new, explicit run_id");
      require(config.operationCount == 100 && config.batchSize == 100,
          "Fixed workload requires op_count=100 and batch_size=100");
      require(config.measurements.size() == 10, "Fixed workload requires ten measurements s0..s9");
      for (int i = 0; i < 10; i++) {
        require(config.measurements.get(i).equals("s" + i) && config.dataTypes.get(i) == TSDataType.INT64,
            "Fixed workload requires s0..s9, all INT64");
      }
      require(config.startTimestamp >= 0 && config.timestampStep == 1
          && config.valueStart == 0 && config.valueStep == 1 && config.prepareSchema,
          "Fixed workload requires explicit start_timestamp, 1 ms steps, values 0+ordinal, and schema preparation");
      int workers = positiveInt(context, "workers", 10);
      sources = positiveInt(context, "sources_per_worker", 50);
      batchesPerSource = positiveInt(context, "batches_per_source", 2400);
      prepareTimeoutMs = positiveInt(context, "prepare_timeout_ms", 300000);
      totalCalls = Math.multiplyExact((long) sources, batchesPerSource);
      Math.addExact(config.startTimestamp, Math.multiplyExact((long) batchesPerSource, 100));
      String signature = config.host + ":" + config.port + "/" + config.devicePath + "/"
          + workers + "/" + sources + "/" + batchesPerSource + "/"
          + config.startTimestamp + "/" + config.aligned;
      run = FixedRunCoordinator.acquire(config.runId, signature, workers);
      worker = run.register();
      workerPath = config.devicePath + ".run_" + config.runId + ".worker_" + worker;
      tablets = new TabletFactory[sources];
      client = createClient();
      client.open(config);
      for (int i = 0; i < sources; i++) {
        String device = source(i);
        client.prepare(config, device);
        client.requireEmpty(device);
        client.prepare(config, warmup(i));
        client.requireEmpty(warmup(i));
        tablets[i] = new TabletFactory(config, device);
      }
    } catch (Exception error) {
      setupFailure = error;
      FixedRunCoordinator.failIfPresent(context.getParameter("run_id", "").trim());
      closeClient();
    }
  }

  WriteClient createClient() {
    return new SessionWriteClient();
  }

  @Override
  public SampleResult runTest(JavaSamplerContext context) {
    String phase = prepared ? "formal" : "prepare";
    String device = prepared ? source((int) (submitted % sources)) : workerPath;
    SampleResult result = new SampleResult();
    result.setSampleLabel("IoTDB fixed " + phase);
    result.setDataType(SampleResult.TEXT);
    result.setContentType("application/json");
    result.setDataEncoding("UTF-8");
    int attempted = 0;
    int acknowledged = 0;
    long first = -1;
    long last = -1;
    result.sampleStart();
    try {
      if (setupFailure != null) {
        throw setupFailure;
      }
      require(client != null && run != null, "Sampler must be initialized first");
      if (!prepared) {
        for (int i = 0; i < sources; i++) {
          Tablet warmup = new TabletFactory(config, warmup(i)).fill(100, config.startTimestamp, 0);
          client.insert(warmup, config.aligned);
        }
        startMillis = run.awaitStart(worker, prepareTimeoutMs);
        prepared = true;
      } else {
        startMillis = run.requireStarted();
        require(submitted < totalCalls, "Fixed workload quota was already reached");
        int sourceIndex = (int) (submitted % sources);
        long firstValue = Math.multiplyExact(submitted / sources, 100);
        first = Math.addExact(config.startTimestamp, firstValue);
        last = Math.addExact(first, 99);
        // Reserve once before I/O. Lost acknowledgements are never replayed.
        submitted++;
        Tablet tablet = tablets[sourceIndex].fill(100, first, firstValue);
        attempted = 100;
        client.insert(tablet, config.aligned);
        acknowledged = 100;
      }
      result.setSuccessful(true);
      result.setResponseCodeOK();
      result.setResponseMessage(phase.equals("prepare") ? "Prepared sources; common start released"
          : "Acknowledged 100 rows / 1000 points");
    } catch (Exception error) {
      if (run != null) {
        run.fail();
      }
      result.setSuccessful(false);
      result.setErrorCount(1);
      result.setResponseCode(setupFailure != null ? "SETUP_ERROR" : "FIXED_WORKLOAD_ERROR");
      String message = error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage());
      String password = config == null ? context.getParameter("password", "") : config.password;
      if (password != null && !password.isEmpty()) {
        message = message.replace(password, "[redacted]");
      }
      result.setResponseMessage(message.length() > 2000 ? message.substring(0, 2000) : message);
      result.setStopTest(true);
    } finally {
      result.sampleEnd();
    }
    result.setSampleCount(1);
    long points = acknowledged * 10L;
    String json = "{\"phase\":\"" + phase + "\",\"device\":\"" + device
        + "\",\"worker\":" + worker + ",\"formal_start_ms\":" + startMillis
        + ",\"timestamp_is_start\":" + result.isStampedAtStart()
        + ",\"requested_rows\":" + (phase.equals("formal") ? 100 : 0)
        + ",\"attempted_rows\":" + attempted + ",\"acknowledged_rows\":" + acknowledged
        + ",\"acknowledged_points\":" + points + ",\"first_timestamp\":" + first
        + ",\"last_timestamp\":" + last + "}";
    result.setResponseData(json, "UTF-8");
    JMeterVariables vars = context.getJMeterVariables();
    if (vars != null) {
      vars.put("iotdb_phase", phase);
      vars.put("iotdb_run_id", config == null ? context.getParameter("run_id", "") : config.runId);
      vars.put("iotdb_formal_start_ms", Long.toString(startMillis));
      vars.put("iotdb_worker", Integer.toString(worker));
      vars.put("iotdb_timestamp_is_start", Boolean.toString(result.isStampedAtStart()));
      vars.put("iotdb_ack_rows", Integer.toString(acknowledged));
      vars.put("iotdb_ack_points", Long.toString(points));
      vars.put("iotdb_attempted_rows", Integer.toString(attempted));
      vars.put("iotdb_device", device);
      vars.put("iotdb_first_timestamp", Long.toString(first));
      vars.put("iotdb_last_timestamp", Long.toString(last));
    }
    return result;
  }

  private String source(int index) {
    return workerPath + ".source_" + index;
  }

  private String warmup(int index) {
    return workerPath + ".warmup_" + index;
  }

  private static int positiveInt(JavaSamplerContext context, String name, int defaultValue) {
    long value = WriteConfig.number(context, name, defaultValue);
    require(value > 0 && value <= Integer.MAX_VALUE, name + " must be a positive integer");
    return (int) value;
  }

  private static void require(boolean value, String message) {
    if (!value) {
      throw new IllegalArgumentException(message);
    }
  }

  @Override
  public void teardownTest(JavaSamplerContext context) {
    closeClient();
    if (run != null && worker >= 0) {
      run.release();
      run = null;
    }
  }

  private void closeClient() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception ignored) {
        // Do not log exception text which may contain credentials.
      } finally {
        client = null;
      }
    }
  }
}
