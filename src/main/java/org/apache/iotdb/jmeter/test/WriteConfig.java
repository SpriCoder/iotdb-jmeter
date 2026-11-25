package org.apache.iotdb.jmeter.test;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

/** Validated, per-thread write settings. Secrets are never included in toString/results. */
final class WriteConfig {
  final String host;
  final int port;
  final String username;
  final String password;
  final String model;
  final String tableDatabase;
  final String tableName;
  final String sourceColumn;
  final String sourceId;
  final boolean useSsl;
  final String trustStore;
  final String trustStorePassword;
  final String devicePath;
  final boolean devicePerThread;
  final String runId;
  final List<String> measurements;
  final List<TSDataType> dataTypes;
  final int operationCount;
  final int batchSize;
  final long startTimestamp;
  final long timestampStep;
  final long valueStart;
  final long valueStep;
  final boolean aligned;
  final boolean prepareSchema;
  final boolean stopOnError;
  final int connectionTimeoutMs;
  final long queryTimeoutMs;
  final boolean rpcCompression;

  WriteConfig(JavaSamplerContext context) {
    host = text(context, "host", "127.0.0.1");
    port = positiveInt(context, "port", 6667);
    require(port <= 65535, "port must be between 1 and 65535");
    username = text(context, "username", "root");
    String suppliedPassword = context.getParameter("password", "");
    password = suppliedPassword.isEmpty() ? System.getenv("IOTDB_PASSWORD") : suppliedPassword;
    require(password != null && !password.isEmpty(),
        "Set IOTDB_PASSWORD or the password parameter before running the sampler");
    model = context.getParameter("model", "tree").trim().toLowerCase(Locale.ROOT);
    require(model.equals("tree") || model.equals("table"), "model must be tree or table");
    tableDatabase = model.equals("table") ? context.getParameter("database", "").trim().toLowerCase(Locale.ROOT)
        : context.getParameter("database", "").trim();
    tableName = context.getParameter("table_name", "jmeter_data").trim().toLowerCase(Locale.ROOT);
    sourceColumn = context.getParameter("source_column", "device_id").trim().toLowerCase(Locale.ROOT);
    sourceId = context.getParameter("source_id", "jmeter_source").trim();
    useSsl = bool(context, "use_ssl", false);
    trustStore = context.getParameter("trust_store", "").trim();
    String suppliedTrustStorePassword = context.getParameter("trust_store_pwd", "");
    trustStorePassword = suppliedTrustStorePassword.isEmpty()
        ? System.getenv("IOTDB_TRUSTSTORE_PASSWORD") : suppliedTrustStorePassword;
    require(trustStore.isEmpty() || (trustStorePassword != null && !trustStorePassword.isEmpty()),
        "Set IOTDB_TRUSTSTORE_PASSWORD when trust_store is specified");
    require(!useSsl || !trustStore.isEmpty(), "Set trust_store when use_ssl=true");
    if (model.equals("table")) {
      requireIdentifier(tableDatabase, "database");
      requireIdentifier(tableName, "table_name");
      requireIdentifier(sourceColumn, "source_column");
      requireIdentifier(sourceId, "source_id");
    }
    String legacyPath = model.equals("tree") ? tableDatabase : "";
    String suppliedPath = context.getParameter("device_path", "").trim();
    devicePath = suppliedPath.isEmpty()
        ? (legacyPath.isEmpty() ? "root.jmeter.device" : legacyPath) : suppliedPath;
    if (model.equals("tree")) {
      require(devicePath.matches("root(?:\\.[A-Za-z_][A-Za-z0-9_]*)+"),
          "device_path must be a root path with simple identifier nodes (no wildcards)");
    }
    devicePerThread = bool(context, "device_per_thread", true);
    runId = context.getParameter("run_id", "").trim();
    require(runId.isEmpty() || runId.matches("[A-Za-z0-9_]+"),
        "run_id must contain only letters, digits and underscores");

    String legacyMeasurement = context.getParameter("measurement", "value");
    String names = context.getParameter("measurements", "").trim();
    if (names.isEmpty()) {
      names = context.containsParameter("measurement") ? legacyMeasurement : "s0";
    }
    List<String> parsedNames = new ArrayList<>();
    for (String name : names.split(",", -1)) {
      name = name.trim();
      if (model.equals("table")) {
        name = name.toLowerCase(Locale.ROOT);
        require(!name.equals("time") && !name.equals(sourceColumn),
            "Table FIELD names must differ from time and source_column");
      }
      require(name.matches("[A-Za-z_][A-Za-z0-9_]*"),
          "measurements must contain comma-separated simple identifiers");
      parsedNames.add(name);
    }
    require(new HashSet<>(parsedNames).size() == parsedNames.size(),
        "measurements must not contain duplicate names");
    measurements = Collections.unmodifiableList(parsedNames);
    require(!model.equals("table") || !sourceColumn.equals("time"), "source_column must differ from time");
    List<TSDataType> parsedTypes = new ArrayList<>();
    for (String type : text(context, "data_types", "INT64").split(",", -1)) {
      switch (type.trim().toUpperCase(Locale.ROOT)) {
        case "BOOLEAN": case "INT32": case "INT64": case "FLOAT":
        case "DOUBLE": case "TEXT": case "STRING":
          parsedTypes.add(TSDataType.valueOf(type.trim().toUpperCase(Locale.ROOT)));
          break;
        default:
          throw new IllegalArgumentException(
              "data_types supports BOOLEAN,INT32,INT64,FLOAT,DOUBLE,TEXT,STRING");
      }
    }
    require(parsedNames.size() == parsedTypes.size(),
        "measurements and data_types must have the same number of entries");
    dataTypes = Collections.unmodifiableList(parsedTypes);
    operationCount = positiveInt(context, "op_count", 100);
    batchSize = Math.min(positiveInt(context, "batch_size", 100), operationCount);
    startTimestamp = context.getParameter("start_timestamp", "").trim().isEmpty()
        ? -1L : number(context, "start_timestamp", -1L);
    require(startTimestamp >= -1, "start_timestamp must be -1 or a nonnegative timestamp in ms");
    timestampStep = positiveLong(context, "timestamp_step_ms", 1);
    Math.multiplyExact(operationCount, timestampStep);
    valueStart = number(context, "value_start", 0);
    valueStep = number(context, "value_step", 1);
    aligned = bool(context, "aligned", false);
    require(!model.equals("table") || !aligned, "aligned applies only to tree model; table columns are not aligned");
    prepareSchema = bool(context, "prepare_schema", true);
    stopOnError = bool(context, "stop_on_error", true);
    connectionTimeoutMs = positiveInt(context, "connection_timeout_ms", 5000);
    queryTimeoutMs = positiveLong(context, "query_timeout_ms", 60000);
    rpcCompression = bool(context, "rpc_compression", false);
  }

  static String text(JavaSamplerContext context, String name, String defaultValue) {
    String value = context.getParameter(name, defaultValue).trim();
    require(!value.isEmpty(), name + " must not be empty");
    return value;
  }

  static long number(JavaSamplerContext context, String name, long defaultValue) {
    try {
      return Long.parseLong(context.getParameter(name, Long.toString(defaultValue)).trim());
    } catch (NumberFormatException error) {
      throw new IllegalArgumentException(name + " must be an integer");
    }
  }

  private static long positiveLong(JavaSamplerContext context, String name, long defaultValue) {
    long value = number(context, name, defaultValue);
    require(value > 0, name + " must be greater than zero");
    return value;
  }

  private static int positiveInt(JavaSamplerContext context, String name, int defaultValue) {
    long value = positiveLong(context, name, defaultValue);
    require(value <= Integer.MAX_VALUE, name + " exceeds the integer range");
    return (int) value;
  }

  private static boolean bool(JavaSamplerContext context, String name, boolean defaultValue) {
    String value = context.getParameter(name, Boolean.toString(defaultValue)).trim();
    require(value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"),
        name + " must be true or false");
    return Boolean.parseBoolean(value);
  }

  private static void requireIdentifier(String value, String name) {
    require(value.matches("[A-Za-z_][A-Za-z0-9_]*"), name + " must be a simple SQL identifier");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
