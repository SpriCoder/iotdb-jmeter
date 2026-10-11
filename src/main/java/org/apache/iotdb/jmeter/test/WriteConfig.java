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
    String legacyPath = context.getParameter("database", "").trim();
    String suppliedPath = context.getParameter("device_path", "").trim();
    devicePath = suppliedPath.isEmpty()
        ? (legacyPath.isEmpty() ? "root.jmeter.device" : legacyPath) : suppliedPath;
    require(devicePath.matches("root(?:\\.[A-Za-z_][A-Za-z0-9_]*)+"),
        "device_path must be a root path with simple identifier nodes (no wildcards)");
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
      require(name.matches("[A-Za-z_][A-Za-z0-9_]*"),
          "measurements must contain comma-separated simple identifiers");
      parsedNames.add(name);
    }
    require(new HashSet<>(parsedNames).size() == parsedNames.size(),
        "measurements must not contain duplicate names");
    measurements = Collections.unmodifiableList(parsedNames);
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

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
