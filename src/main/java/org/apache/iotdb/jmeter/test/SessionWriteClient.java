package org.apache.iotdb.jmeter.test;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;
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
  private static final Object[] DATABASE_LOCKS = new Object[32];
  static {
    for (int i = 0; i < SCHEMA_LOCKS.length; i++) {
      SCHEMA_LOCKS[i] = new Object();
    }
    for (int i = 0; i < DATABASE_LOCKS.length; i++) {
      DATABASE_LOCKS[i] = new Object();
    }
  }

  private Session session;
  private ITableSession tableSession;
  private WriteConfig tableConfig;
  private boolean tablePrepared;

  @Override
  public void open(WriteConfig config) throws Exception {
    if (config.model.equals("table")) {
      openTable(config);
      return;
    }
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

  private void openTable(WriteConfig config) throws Exception {
    tableConfig = config;
    Session bootstrap = new Session.Builder()
        .host(config.host).port(config.port)
        .username(config.username).password(config.password)
        .sqlDialect("table").enableAutoFetch(false).enableRedirection(false).maxRetryCount(0)
        .timeOut(config.queryTimeoutMs).build();
    try {
      bootstrap.open(config.rpcCompression, config.connectionTimeoutMs);
      if (!"ms".equals(bootstrap.getTimestampPrecision())) {
        throw new IllegalArgumentException("This sampler requires server timestamp_precision=ms");
      }
      String databaseKey = config.host + ":" + config.port + "/" + config.tableDatabase;
      Object lock = DATABASE_LOCKS[(databaseKey.hashCode() & Integer.MAX_VALUE) % DATABASE_LOCKS.length];
      synchronized (lock) {
        if (config.prepareSchema) {
          bootstrap.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS " + config.tableDatabase);
        }
      }
    } finally {
      bootstrap.close();
    }
    tableSession = new TableSessionBuilder()
        .nodeUrls(Collections.singletonList(config.host + ":" + config.port))
        .username(config.username).password(config.password).database(config.tableDatabase)
        .queryTimeoutInMs(config.queryTimeoutMs).connectionTimeoutInMs(config.connectionTimeoutMs)
        .enableCompression(config.rpcCompression)
        .enableRedirection(false).enableAutoFetch(false).maxRetryCount(0).build();
  }

  @Override
  public void prepare(WriteConfig config, String device) throws Exception {
    if (config.model.equals("table")) {
      prepareTable(config);
      return;
    }
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

  private void prepareTable(WriteConfig config) throws Exception {
    if (tablePrepared) {
      return;
    }
    String key = config.host + ":" + config.port + "/" + config.tableDatabase + "/" + config.tableName;
    Object lock = SCHEMA_LOCKS[(key.hashCode() & Integer.MAX_VALUE) % SCHEMA_LOCKS.length];
    synchronized (lock) {
      StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(config.tableName)
          .append(" (").append(config.sourceColumn).append(" STRING TAG");
      for (int index = 0; index < config.measurements.size(); index++) {
        sql.append(", ").append(config.measurements.get(index)).append(' ')
            .append(config.dataTypes.get(index)).append(" FIELD");
      }
      sql.append(')');
      tableSession.executeNonQueryStatement(sql.toString());
      validateTableSchema(tableSession, config.tableName, config.sourceColumn,
          config.measurements, config.dataTypes);
      tablePrepared = true;
    }
  }

  static void validateTableSchema(ITableSession session, String table, String source,
      List<String> measurements, List<TSDataType> dataTypes) throws Exception {
    Map<String, String> actual = new HashMap<>();
    try (SessionDataSet data = session.executeQueryStatement("DESCRIBE " + table)) {
      SessionDataSet.DataIterator iterator = data.iterator();
      while (iterator.next()) {
        actual.put(iterator.getString("ColumnName"),
            iterator.getString("DataType") + "/" + iterator.getString("Category"));
      }
    }
    if (!"STRING/TAG".equals(actual.get(source))) {
      throw new IllegalArgumentException("Source column must be STRING TAG: " + source);
    }
    if (!"TIMESTAMP/TIME".equals(actual.get("time"))) {
      throw new IllegalArgumentException("Table must have its standard TIMESTAMP time column");
    }
    java.util.Set<String> expectedColumns = new java.util.HashSet<>(measurements);
    expectedColumns.add(source);
    expectedColumns.add("time");
    for (Map.Entry<String, String> entry : actual.entrySet()) {
      if (entry.getValue().endsWith("/TAG") && !entry.getKey().equals(source)) {
        throw new IllegalArgumentException("Unexpected additional TAG column: " + entry.getKey());
      }
      if (!expectedColumns.contains(entry.getKey())) {
        throw new IllegalArgumentException("Unexpected additional table column: " + entry.getKey());
      }
    }
    for (int index = 0; index < measurements.size(); index++) {
      String expected = dataTypes.get(index).name() + "/FIELD";
      if (!expected.equals(actual.get(measurements.get(index)))) {
        throw new IllegalArgumentException("Table FIELD metadata mismatch: " + measurements.get(index));
      }
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
  public void requireEmpty(String device) throws Exception {
    if (tableSession != null) {
      String sql = "SELECT time FROM " + tableConfig.tableName + " WHERE " + tableConfig.sourceColumn
          + " = '" + device + "' LIMIT 1";
      try (SessionDataSet data = tableSession.executeQueryStatement(sql)) {
        if (data.hasNext()) {
          throw new IllegalArgumentException("Run namespace contains data; choose a new run_id");
        }
      }
      return;
    }
    try (SessionDataSet data = session.executeQueryStatement("SELECT * FROM " + device + " LIMIT 1")) {
      if (data.hasNext()) {
        throw new IllegalArgumentException("Run namespace contains data; choose a new run_id");
      }
    }
  }

  @Override
  public void insert(Tablet tablet, boolean aligned) throws Exception {
    if (tableSession != null) {
      tableSession.insert(tablet);
      return;
    }
    if (aligned) {
      session.insertAlignedTablet(tablet, true);
    } else {
      session.insertTablet(tablet, true);
    }
  }

  @Override
  public void close() throws Exception {
    if (tableSession != null) {
      try {
        tableSession.close();
      } finally {
        tableSession = null;
        tableConfig = null;
        tablePrepared = false;
      }
    }
    if (session != null) {
      try {
        session.close();
      } finally {
        session = null;
      }
    }
  }
}
