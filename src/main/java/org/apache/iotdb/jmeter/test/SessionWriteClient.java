package org.apache.iotdb.jmeter.test;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** All schema work and connection setup happen before JMeter starts sample timing. */
final class SessionWriteClient implements WriteClient {
  private static final Object[] SCHEMA_LOCKS = new Object[64];
  static {
    for (int i = 0; i < SCHEMA_LOCKS.length; i++) {
      SCHEMA_LOCKS[i] = new Object();
    }
  }

  private Session session;

  @Override
  public void open(WriteConfig config) throws Exception {
    session = new Session.Builder()
        .host(config.host).port(config.port)
        .username(config.username).password(config.password)
        // Leader-cache fallback can resend a failed write even with zero
        // retries. Keep routing on the configured endpoint for this benchmark.
        .sqlDialect("tree").enableAutoFetch(false).enableRedirection(false).maxRetryCount(0)
        .timeOut(config.queryTimeoutMs).build();
    session.open(config.rpcCompression, config.connectionTimeoutMs);
    session.setQueryTimeout(config.queryTimeoutMs);
    if (!"ms".equals(session.getTimestampPrecision())) {
      throw new IllegalArgumentException(
          "This sampler requires server timestamp_precision=ms");
    }
  }

  @Override
  public void prepare(WriteConfig config, String device) throws Exception {
    synchronized (SCHEMA_LOCKS[(device.hashCode() & Integer.MAX_VALUE) % SCHEMA_LOCKS.length]) {
      Map<String, String> existing = readSchema(device);
      validateTypes(config, device, existing, false);
      validateAlignment(config, device);
      List<String> missing = new ArrayList<>();
      List<TSDataType> missingTypes = new ArrayList<>();
      for (int i = 0; i < config.measurements.size(); i++) {
        if (!existing.containsKey(device + "." + config.measurements.get(i))) {
          missing.add(config.measurements.get(i));
          missingTypes.add(config.dataTypes.get(i));
        }
      }
      try {
        if (config.aligned && !missing.isEmpty()) {
          session.createAlignedTimeseries(device, missing, missingTypes,
              Collections.nCopies(missing.size(), TSEncoding.PLAIN),
              Collections.nCopies(missing.size(), CompressionType.LZ4), null);
        } else {
          for (int i = 0; i < missing.size(); i++) {
            session.createTimeseries(device + "." + missing.get(i), missingTypes.get(i),
                TSEncoding.PLAIN, CompressionType.LZ4);
          }
        }
      } catch (StatementExecutionException creationError) {
        // A different engine may have created the same schema concurrently.
        // Ignore only a race that left exactly the requested schema in place.
        try {
          validateTypes(config, device, readSchema(device), true);
          validateAlignment(config, device);
        } catch (Exception validationError) {
          creationError.addSuppressed(validationError);
          throw creationError;
        }
      }
      validateTypes(config, device, readSchema(device), true);
      validateAlignment(config, device);
    }
  }

  private Map<String, String> readSchema(String device) throws Exception {
    Map<String, String> types = new HashMap<>();
    try (SessionDataSet data = session.executeQueryStatement("SHOW TIMESERIES " + device + ".*")) {
      SessionDataSet.DataIterator iterator = data.iterator();
      while (iterator.next()) {
        types.put(iterator.getString("Timeseries"), iterator.getString("DataType"));
      }
    }
    return types;
  }

  private static void validateTypes(
      WriteConfig config, String device, Map<String, String> existing, boolean requireAll) {
    for (int i = 0; i < config.measurements.size(); i++) {
      String path = device + "." + config.measurements.get(i);
      String actual = existing.get(path);
      if (actual == null && requireAll) {
        throw new IllegalArgumentException("Missing measurement after schema preparation: " + path);
      }
      if (actual != null && !config.dataTypes.get(i).name().equals(actual)) {
        throw new IllegalArgumentException("Schema type mismatch for " + path
            + ": expected " + config.dataTypes.get(i) + ", found " + actual);
      }
    }
  }

  private void validateAlignment(WriteConfig config, String device) throws Exception {
    try (SessionDataSet data = session.executeQueryStatement("SHOW DEVICES " + device)) {
      SessionDataSet.DataIterator iterator = data.iterator();
      while (iterator.next()) {
        if (device.equals(iterator.getString("Device"))
            && config.aligned != Boolean.parseBoolean(iterator.getString("IsAligned"))) {
          throw new IllegalArgumentException("Existing device alignment differs from aligned parameter: " + device);
        }
      }
    }
  }

  @Override
  public void insert(Tablet tablet, boolean aligned) throws Exception {
    if (aligned) {
      session.insertAlignedTablet(tablet, true);
    } else {
      session.insertTablet(tablet, true);
    }
  }

  @Override
  public void close() throws Exception {
    if (session != null) {
      try {
        session.close();
      } finally {
        session = null;
      }
    }
  }
}
