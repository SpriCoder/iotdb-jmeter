package org.apache.iotdb.jmeter.test;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Optional real-server checks: mvn -Pintegration verify -Dit.host=... -Dit.port=... . */
public class IoTDBWriteIT {
  private static final long START_TIMESTAMP = 1700000000000L;
  private static final String NAMES = "s0,s1,s2,s3,s4,s5,s6";
  private static final String TYPES = "BOOLEAN,INT32,INT64,FLOAT,DOUBLE,TEXT,STRING";

  @Test
  public void readsBackEveryTypeAcrossPartialBatchesAndRepeatedSamples() throws Exception {
    verifyWrites(false);
  }

  @Test
  public void readsBackAlignedWritesAndAlignmentMetadata() throws Exception {
    verifyWrites(true);
  }

  @Test
  public void rejectsExistingSchemaTypeMismatchBeforeWriting() throws Exception {
    Connection connection = connection();
    String device = uniqueDevice();
    Arguments args = parameters(connection, device, false);
    args.removeArgument("device_per_thread");
    args.removeArgument("measurements");
    args.removeArgument("data_types");
    args.addArgument("device_per_thread", "false");
    args.addArgument("measurements", "s0");
    args.addArgument("data_types", "INT64");
    JavaSamplerContext context = new JavaSamplerContext(args);
    JmeterTest sampler = new JmeterTest();
    Session reader = connection.open();
    try {
      reader.createTimeseries(device + ".s0", TSDataType.DOUBLE,
          TSEncoding.PLAIN, CompressionType.LZ4);
      sampler.setupTest(context);
      SampleResult result = sampler.runTest(context);
      assertFalse(result.isSuccessful());
      assertEquals("SETUP_ERROR", result.getResponseCode());
      assertTrue(result.getResponseMessage(), result.getResponseMessage().contains("Schema type mismatch"));
      assertTrue(result.isStopThread());
      assertEquals(0, counter(result, "attempted_rows"));
      assertEquals(0, counter(result, "acknowledged_rows"));
      try (SessionDataSet data = reader.executeQueryStatement("SELECT s0 FROM " + device)) {
        assertFalse(data.hasNext());
      }
    } finally {
      sampler.teardownTest(context);
      try {
        reader.deleteTimeseries(device + ".s0");
      } finally {
        reader.close();
      }
    }
  }

  private static void verifyWrites(boolean aligned) throws Exception {
    Connection connection = connection();
    String baseDevice = uniqueDevice();
    JavaSamplerContext context = new JavaSamplerContext(parameters(connection, baseDevice, aligned));
    JmeterTest sampler = new JmeterTest();
    String writtenDevice = null;
    Session reader = connection.open();
    try {
      sampler.setupTest(context);
      SampleResult first = sampler.runTest(context);
      writtenDevice = text(first, "device");
      assertSuccessfulSample(first, 5, 35, 3);
      assertTrue(writtenDevice, writtenDevice.startsWith(baseDevice + ".run_e2e.t"));
      assertEquals(START_TIMESTAMP, counter(first, "first_timestamp"));
      assertEquals(START_TIMESTAMP + 4, counter(first, "last_timestamp"));

      SampleResult second = sampler.runTest(context);
      assertSuccessfulSample(second, 5, 35, 3);
      assertEquals(writtenDevice, text(second, "device"));
      assertEquals(START_TIMESTAMP + 5, counter(second, "first_timestamp"));
      assertEquals(START_TIMESTAMP + 9, counter(second, "last_timestamp"));
      sampler.teardownTest(context);

      int rows = 0;
      try (SessionDataSet data = reader.executeQueryStatement("SELECT " + NAMES + " FROM " + writtenDevice)) {
        while (data.hasNext()) {
          RowRecord row = data.next();
          assertEquals(START_TIMESTAMP + rows, row.getTimestamp());
          List<Field> values = row.getFields();
          assertEquals(7, values.size());
          assertEquals((rows & 1) == 0, values.get(0).getBoolV());
          assertEquals(rows + 1, values.get(1).getIntV());
          assertEquals(rows + 2L, values.get(2).getLongV());
          assertEquals(rows + 3.0f, values.get(3).getFloatV(), 0.0f);
          assertEquals(rows + 4.0, values.get(4).getDoubleV(), 0.0);
          assertEquals("v" + (rows + 5), values.get(5).getStringValue());
          assertEquals("v" + (rows + 6), values.get(6).getStringValue());
          rows++;
        }
      }
      assertEquals(10, rows);
      try (SessionDataSet data = reader.executeQueryStatement("SHOW DEVICES " + writtenDevice)) {
        SessionDataSet.DataIterator iterator = data.iterator();
        assertTrue(iterator.next());
        assertEquals(writtenDevice, iterator.getString("Device"));
        assertEquals(aligned, Boolean.parseBoolean(iterator.getString("IsAligned")));
        assertFalse(iterator.next());
      }
    } finally {
      sampler.teardownTest(context);
      try {
        if (writtenDevice != null) {
          List<String> paths = new ArrayList<>();
          for (String name : NAMES.split(",")) {
            paths.add(writtenDevice + "." + name);
          }
          reader.deleteTimeseries(paths);
        }
      } finally {
        reader.close();
      }
    }
  }

  private static Arguments parameters(Connection connection, String device, boolean aligned) {
    Arguments args = new Arguments();
    args.addArgument("host", connection.host);
    args.addArgument("port", Integer.toString(connection.port));
    args.addArgument("username", connection.username);
    args.addArgument("password", connection.password);
    args.addArgument("device_path", device);
    args.addArgument("measurements", NAMES);
    args.addArgument("data_types", TYPES);
    args.addArgument("op_count", "5");
    args.addArgument("batch_size", "2");
    args.addArgument("start_timestamp", Long.toString(START_TIMESTAMP));
    args.addArgument("timestamp_step_ms", "1");
    args.addArgument("aligned", Boolean.toString(aligned));
    args.addArgument("run_id", "e2e");
    return args;
  }

  private static void assertSuccessfulSample(SampleResult result, int rows, long points, int batches) {
    assertTrue(result.getResponseCode() + ": " + result.getResponseMessage(), result.isSuccessful());
    assertEquals(1, result.getSampleCount());
    assertEquals(0, result.getErrorCount());
    assertEquals(rows, counter(result, "requested_rows"));
    assertEquals(rows, counter(result, "attempted_rows"));
    assertEquals(rows, counter(result, "acknowledged_rows"));
    assertEquals(points, counter(result, "acknowledged_points"));
    assertEquals(0, counter(result, "failed_rows"));
    assertEquals(0, counter(result, "unattempted_rows"));
    assertEquals(batches, counter(result, "batches"));
  }

  private static long counter(SampleResult result, String name) {
    Matcher matcher = Pattern.compile("\"" + name + "\":(-?\\d+)").matcher(result.getResponseDataAsString());
    assertTrue(result.getResponseDataAsString(), matcher.find());
    return Long.parseLong(matcher.group(1));
  }

  private static String text(SampleResult result, String name) {
    Matcher matcher = Pattern.compile("\"" + name + "\":\"([^\"]*)\"").matcher(result.getResponseDataAsString());
    assertTrue(result.getResponseDataAsString(), matcher.find());
    return matcher.group(1);
  }

  private static String uniqueDevice() {
    return "root.jmeter_it.test_" + UUID.randomUUID().toString().replace("-", "");
  }

  private static Connection connection() {
    String host = setting("it.host", "IOTDB_IT_HOST", null);
    Assume.assumeTrue("Set it.host or IOTDB_IT_HOST to enable real-server verification",
        host != null && !host.isEmpty());
    String password = setting("it.password", "IOTDB_PASSWORD", null);
    assertNotNull("Set it.password or IOTDB_PASSWORD for the test server", password);
    assertFalse("The integration password must not be empty", password.isEmpty());
    return new Connection(host, Integer.parseInt(setting("it.port", "IOTDB_IT_PORT", "6667")),
        setting("it.username", "IOTDB_IT_USERNAME", "root"), password);
  }

  private static String setting(String property, String environment, String fallback) {
    String value = System.getProperty(property);
    if (value == null || value.isEmpty()) {
      value = System.getenv(environment);
    }
    return value == null || value.isEmpty() ? fallback : value;
  }

  private static final class Connection {
    final String host;
    final int port;
    final String username;
    final String password;

    Connection(String host, int port, String username, String password) {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
    }

    Session open() throws Exception {
      Session session = new Session.Builder().host(host).port(port)
          .username(username).password(password).enableAutoFetch(false).maxRetryCount(0)
          .sqlDialect("tree").build();
      try {
        session.open(false, 5000);
        return session;
      } catch (Exception error) {
        session.close();
        throw error;
      }
    }
  }
}
