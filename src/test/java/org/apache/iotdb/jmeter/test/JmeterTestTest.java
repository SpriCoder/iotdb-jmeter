package org.apache.iotdb.jmeter.test;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class JmeterTestTest {
  @Test
  public void writesOneHundredRowTenMeasurementTabletsWithNonoverlappingSamples() {
    JavaSamplerContext defaults = new JavaSamplerContext(new JmeterTest().getDefaultParameters());
    assertEquals("100", defaults.getParameter("op_count"));
    assertEquals("100", defaults.getParameter("batch_size"));
    assertEquals("s0", defaults.getParameter("measurements"));

    for (boolean aligned : new boolean[] {false, true}) {
      FakeClient client = new FakeClient();
      TestSampler sampler = new TestSampler(client);
      Arguments args = new Arguments();
      args.addArgument("password", "test_password_placeholder");
      args.addArgument("measurements", "s0,s1,s2,s3,s4,s5,s6,s7,s8,s9");
      args.addArgument("data_types", "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64");
      args.addArgument("start_timestamp", "100");
      args.addArgument("aligned", Boolean.toString(aligned));
      // Leave row counts absent to exercise WriteConfig's defaults as well.
      JavaSamplerContext context = new JavaSamplerContext(args);
      sampler.setupTest(context);
      SampleResult first = sampler.runTest(context);
      SampleResult second = sampler.runTest(context);
      sampler.teardownTest(context);

      for (SampleResult result : Arrays.asList(first, second)) {
        assertTrue(result.getResponseMessage(), result.isSuccessful());
        assertEquals(1, result.getSampleCount());
        assertEquals(0, result.getErrorCount());
        String json = result.getResponseDataAsString();
        assertTrue(json, json.contains("\"requested_rows\":100,"));
        assertTrue(json, json.contains("\"attempted_rows\":100,"));
        assertTrue(json, json.contains("\"acknowledged_rows\":100,"));
        assertTrue(json, json.contains("\"acknowledged_points\":1000,"));
        assertTrue(json, json.contains("\"batches\":1,"));
      }
      assertEquals(Arrays.asList(100, 100), client.batchRows);
      assertEquals(2, client.inserts);
      assertEquals(aligned, client.aligned);
      assertEquals(200, client.timestamps.size());
      assertEquals(200, new HashSet<>(client.timestamps).size());
      assertEquals(100L, (long) client.timestamps.get(0));
      assertEquals(199L, (long) client.timestamps.get(99));
      assertEquals(200L, (long) client.timestamps.get(100));
      assertEquals(299L, (long) client.timestamps.get(199));
      for (int row = 0; row < client.values.size(); row++) {
        assertEquals(10, client.values.get(row).size());
        for (int column = 0; column < 10; column++) {
          assertEquals((long) row + column, client.values.get(row).get(column));
        }
      }
    }
  }

  @Test
  public void writesPartialFinalBatchAndReusesOneSessionAcrossSamples() {
    FakeClient client = new FakeClient();
    TestSampler sampler = new TestSampler(client);
    JavaSamplerContext context = context("measurements", "a,b", "data_types", "INT64,DOUBLE");
    sampler.setupTest(context);
    SampleResult first = sampler.runTest(context);
    SampleResult second = sampler.runTest(context);
    sampler.teardownTest(context);

    assertTrue(first.getResponseMessage(), first.isSuccessful());
    assertTrue(second.isSuccessful());
    assertEquals(1, first.getSampleCount());
    assertEquals(0, first.getErrorCount());
    assertEquals(Arrays.asList(2, 2, 1, 2, 2, 1), client.batchRows);
    assertEquals(1, client.opens);
    assertEquals(1, client.prepares);
    assertEquals(1, client.closes);
    assertEquals(10, client.timestamps.size());
    assertEquals(100L, (long) client.timestamps.get(0));
    assertEquals(109L, (long) client.timestamps.get(9));
    assertEquals(9L, client.values.get(9).get(0));
    assertEquals(10.0, client.values.get(9).get(1));
    assertTrue(first.getResponseDataAsString().contains("\"acknowledged_points\":10"));
  }

  @Test
  public void unconfirmedRequestFailsAndItsEntireSampleRangeIsNeverReplayed() {
    FakeClient client = new FakeClient();
    client.failInsert = 2;
    TestSampler sampler = new TestSampler(client);
    JavaSamplerContext context = context("stop_on_error", "false");
    sampler.setupTest(context);
    SampleResult failure = sampler.runTest(context);
    SampleResult next = sampler.runTest(context);
    sampler.teardownTest(context);

    assertFalse(failure.isSuccessful());
    assertEquals(1, failure.getSampleCount());
    assertEquals(1, failure.getErrorCount());
    assertFalse(failure.isStopThread());
    String json = failure.getResponseDataAsString();
    assertTrue(json, json.contains("\"acknowledged_rows\":2"));
    assertTrue(json, json.contains("\"attempted_rows\":4"));
    assertTrue(json, json.contains("\"unattempted_rows\":1"));
    assertTrue(next.isSuccessful());
    // The failed call included 102/103. The next sample starts at 105,
    // including when the server accepted the failed call before a lost reply.
    assertEquals(105L, (long) client.timestamps.get(4));
    assertEquals(5L, client.values.get(4).get(0));
  }

  @Test
  public void defaultStopsTheThreadAfterAWriteError() {
    FakeClient client = new FakeClient();
    client.failInsert = 1;
    TestSampler sampler = new TestSampler(client);
    JavaSamplerContext context = context();
    sampler.setupTest(context);
    SampleResult result = sampler.runTest(context);
    assertFalse(result.isSuccessful());
    assertTrue(result.isStopThread());
    assertEquals(1, result.getSampleCount());
    assertEquals(1, result.getErrorCount());
    sampler.teardownTest(context);
  }

  @Test
  public void failedOpenIsReportedAndClientIsClosed() {
    FakeClient client = new FakeClient();
    client.failOpen = true;
    TestSampler sampler = new TestSampler(client);
    JavaSamplerContext context = context();
    sampler.setupTest(context);
    SampleResult result = sampler.runTest(context);
    sampler.teardownTest(context);
    assertFalse(result.isSuccessful());
    assertEquals("SETUP_ERROR", result.getResponseCode());
    assertTrue(result.isStopThread());
    assertEquals(1, client.closes);
    assertEquals(0, client.inserts);
  }

  @Test
  public void validatesSettingsBeforeOpeningAnyConnection() {
    String[][] invalid = {
        {"op_count", "zero"}, {"op_count", "0"}, {"batch_size", "-1"},
        {"port", "65536"}, {"aligned", "yes"}, {"timestamp_step_ms", "0"},
        {"start_timestamp", "-2"}, {"run_id", "bad.id"},
        {"device_path", "root.x.*"}, {"measurements", "a,a", "data_types", "INT64,INT64"},
        {"measurements", "a,b", "data_types", "INT64"}, {"data_types", "VECTOR"}
    };
    for (String[] settings : invalid) {
      FakeClient client = new FakeClient();
      TestSampler sampler = new TestSampler(client);
      JavaSamplerContext context = context(settings);
      sampler.setupTest(context);
      SampleResult result = sampler.runTest(context);
      assertFalse(Arrays.toString(settings), result.isSuccessful());
      assertEquals(Arrays.toString(settings), 0, client.opens);
      assertEquals(1, result.getSampleCount());
      assertTrue(result.isStopThread());
    }
  }

  @Test
  public void usesDistinctDefaultDevicesAndChecksAlignmentBeforeWriting() {
    FakeClient firstClient = new FakeClient();
    FakeClient secondClient = new FakeClient();
    TestSampler first = new TestSampler(firstClient);
    TestSampler second = new TestSampler(secondClient);
    JavaSamplerContext context = context("device_per_thread", "true", "aligned", "true");
    first.setupTest(context);
    second.setupTest(context);
    first.runTest(context);
    second.runTest(context);
    assertNotEquals(firstClient.device, secondClient.device);
    assertTrue(firstClient.device.startsWith("root.test.device.run_"));
    assertTrue(firstClient.aligned);
    first.teardownTest(context);
    second.teardownTest(context);
  }

  @Test
  public void sharedDeviceConcurrentWritersReserveNonoverlappingRanges() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    List<FakeClient> clients = Arrays.asList(new FakeClient(), new FakeClient());
    List<Callable<Void>> tasks = new ArrayList<>();
    String path = "root.test.shared_" + Long.toUnsignedString(System.nanoTime());
    for (FakeClient client : clients) {
      tasks.add(() -> {
        TestSampler sampler = new TestSampler(client);
        JavaSamplerContext context = context("device_path", path,
            "device_per_thread", "false", "timestamp_step_ms", "3");
        sampler.setupTest(context);
        for (int i = 0; i < 20; i++) {
          assertTrue(sampler.runTest(context).isSuccessful());
        }
        sampler.teardownTest(context);
        return null;
      });
    }
    try {
      for (Future<Void> future : workers.invokeAll(tasks)) {
        future.get();
      }
      Set<Long> unique = new HashSet<>();
      for (FakeClient client : clients) {
        assertEquals(100, client.timestamps.size());
        for (Long timestamp : client.timestamps) {
          assertTrue("Timestamp reused: " + timestamp, unique.add(timestamp));
        }
      }
      assertEquals(200, unique.size());
    } finally {
      workers.shutdownNow();
    }
  }

  @Test
  public void rejectsValueAndTimestampOverflowBeforeAnySubmission() {
    for (String[] settings : new String[][] {
        {"data_types", "INT32", "value_start", "2147483646"},
        {"value_start", Long.toString(Long.MAX_VALUE - 2)},
        {"start_timestamp", Long.toString(Long.MAX_VALUE - 2)}
    }) {
      FakeClient client = new FakeClient();
      TestSampler sampler = new TestSampler(client);
      JavaSamplerContext context = context(settings);
      sampler.setupTest(context);
      SampleResult result = sampler.runTest(context);
      assertFalse(result.isSuccessful());
      assertEquals("CONFIG_ERROR", result.getResponseCode());
      assertEquals(0, client.inserts);
      sampler.teardownTest(context);
    }
  }

  @Test
  public void redactsPasswordsFromErrorMessagesAndSamplerData() {
    FakeClient client = new FakeClient();
    client.failInsert = 1;
    client.errorMessage = "denied: test_password_placeholder";
    TestSampler sampler = new TestSampler(client);
    JavaSamplerContext context = context();
    sampler.setupTest(context);
    SampleResult result = sampler.runTest(context);
    assertTrue(result.getResponseMessage().contains("[redacted]"));
    assertFalse(result.getResponseMessage().contains("test_password_placeholder"));
    assertFalse(result.getSamplerData().contains("test_password_placeholder"));
    assertFalse(result.getResponseDataAsString().contains("test_password_placeholder"));
    sampler.teardownTest(context);
  }

  static JavaSamplerContext context(String... overrides) {
    Arguments args = new Arguments();
    java.util.Map<String, String> values = new java.util.LinkedHashMap<>();
    values.put("password", "test_password_placeholder");
    values.put("device_path", "root.test.device");
    values.put("device_per_thread", "true");
    values.put("op_count", "5");
    values.put("batch_size", "2");
    values.put("start_timestamp", "100");
    for (int i = 0; i < overrides.length; i += 2) {
      values.put(overrides[i], overrides[i + 1]);
    }
    for (java.util.Map.Entry<String, String> entry : values.entrySet()) {
      args.addArgument(entry.getKey(), entry.getValue());
    }
    return new JavaSamplerContext(args);
  }

  private static final class TestSampler extends JmeterTest {
    private final FakeClient client;
    TestSampler(FakeClient client) { this.client = client; }
    @Override WriteClient createClient() { return client; }
  }

  private static final class FakeClient implements WriteClient {
    int opens;
    int prepares;
    int closes;
    int inserts;
    int failInsert = -1;
    boolean failOpen;
    boolean aligned;
    String errorMessage = "simulated lost reply";
    String device;
    final List<Integer> batchRows = new ArrayList<>();
    final List<Long> timestamps = new ArrayList<>();
    final List<List<Object>> values = new ArrayList<>();

    @Override public void open(WriteConfig config) throws Exception {
      opens++;
      if (failOpen) { throw new Exception("simulated connect failure"); }
    }
    @Override public void prepare(WriteConfig config, String device) { prepares++; this.device = device; }
    @Override public void insert(Tablet tablet, boolean aligned) throws Exception {
      this.aligned = aligned;
      inserts++;
      batchRows.add(tablet.getRowSize());
      for (int row = 0; row < tablet.getRowSize(); row++) {
        timestamps.add(tablet.getTimestamp(row));
        List<Object> snapshot = new ArrayList<>();
        for (int column = 0; column < tablet.getSchemas().size(); column++) {
          snapshot.add(tablet.getValue(row, column));
        }
        values.add(snapshot);
      }
      if (inserts == failInsert) { throw new Exception(errorMessage); }
    }
    @Override public void close() { closes++; }
  }
}
