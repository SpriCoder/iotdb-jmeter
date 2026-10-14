package org.apache.iotdb.jmeter.test;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Independent, read-only reconciliation for the fixed 100-row, ten-INT64 workload. */
public final class FixedWorkloadReadback {
  private static final String HEADER = "device\trows\tfirst_timestamp\tlast_timestamp";
  private static final String NAMES = "s0,s1,s2,s3,s4,s5,s6,s7,s8,s9";

  private FixedWorkloadReadback() {}

  public static void main(String[] args) {
    int status = run(args);
    if (status != 0) {
      System.exit(status);
    }
  }

  static int run(String[] args) {
    Report report = new Report();
    Path output = extractPath(args, "--output");
    String password = System.getenv("IOTDB_PASSWORD");
    Session session = null;
    try {
      Options options = parseOptions(args);
      output = options.output;
      if (options.manifest.equals(output)
          || (Files.exists(output) && Files.isSameFile(options.manifest, output))) {
        System.err.println("Readback output must not replace its input manifest.");
        return 2;
      }
      byte[] raw = Files.readAllBytes(options.manifest);
      report.manifestSha256 = sha256(raw);
      List<Entry> entries = parseManifest(raw);
      require(password != null && !password.isEmpty(), "Set IOTDB_PASSWORD before running readback");
      session = new Session.Builder().host(options.host).port(options.port)
          .username(options.username).password(password).sqlDialect("tree")
          .enableAutoFetch(false).enableRedirection(false).maxRetryCount(0)
          .timeOut(60000).build();
      session.open(false, 5000);
      session.setQueryTimeout(60000);
      require("ms".equals(session.getTimestampPrecision()), "Server timestamp precision must be ms");
      report.serverVersion = serverVersion(session);
      for (Entry entry : entries) {
        try {
          if (verifyDevice(session, entry, options.aligned, report.errors)) {
            report.verifiedDevices++;
            report.verifiedPoints = Math.addExact(report.verifiedPoints, Math.multiplyExact(entry.rows, 10L));
          }
        } catch (ValidationFailure error) {
          report.errors.add(entry.device + ": " + error.getMessage());
        } catch (Exception error) {
          report.errors.add(entry.device + ": query failed (" + error.getClass().getSimpleName() + ")");
        }
      }
      report.passed = report.errors.isEmpty() && report.verifiedDevices == entries.size();
    } catch (ValidationFailure error) {
      // These messages are generated here, never taken from server exceptions.
      report.errors.add(error.getMessage());
    } catch (Exception error) {
      report.errors.add("Readback failed (" + error.getClass().getSimpleName() + ")");
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (Exception error) {
          report.passed = false;
          report.errors.add("Reader Session close failed (" + error.getClass().getSimpleName() + ")");
        }
      }
    }
    if (output == null) {
      System.err.println("Readback requires --manifest <TSV> --output <JSON>.");
      return 2;
    }
    try {
      // Parsing may have failed before the regular options/alias check above.
      // Protect every manifest argument before writing even a failure report.
      for (int index = 0; index + 1 < args.length; index++) {
        if ("--manifest".equals(args[index])) {
          Path manifest = Paths.get(args[index + 1]).toAbsolutePath().normalize();
          if (manifest.equals(output) || (Files.exists(output) && Files.isSameFile(manifest, output))) {
            System.err.println("Readback output must not replace its input manifest.");
            return 2;
          }
        }
      }
      Path parent = output.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      Files.write(output, report.json(password).getBytes(StandardCharsets.UTF_8));
    } catch (Exception error) {
      System.err.println("Unable to save readback JSON (" + error.getClass().getSimpleName() + ").");
      return 2;
    }
    return report.passed ? 0 : 1;
  }

  private static String serverVersion(Session session) throws Exception {
    try (SessionDataSet data = session.executeQueryStatement("SHOW VERSION")) {
      require(data.hasNext(), "SHOW VERSION returned no row");
      RowRecord row = data.next();
      require(!row.getFields().isEmpty(), "SHOW VERSION returned no version");
      Field version = row.getFields().get(0);
      require(version != null && version.getDataType() != null, "SHOW VERSION returned a null version");
      String value = version.getStringValue();
      require(value != null && !value.trim().isEmpty(), "SHOW VERSION returned an empty version");
      return value.length() > 512 ? value.substring(0, 512) : value;
    }
  }

  private static boolean verifyDevice(Session session, Entry entry, boolean aligned, List<String> errors)
      throws Exception {
    int initialErrors = errors.size();
    Map<String, String> types = new HashMap<>();
    // The path is strictly validated before a fixed, single-level wildcard is appended.
    try (SessionDataSet data = session.executeQueryStatement("SHOW TIMESERIES " + entry.device + ".*")) {
      SessionDataSet.DataIterator iterator = data.iterator();
      while (iterator.next()) {
        types.put(iterator.getString("Timeseries"), iterator.getString("DataType"));
      }
    }
    for (int column = 0; column < 10; column++) {
      if (!"INT64".equals(types.get(entry.device + ".s" + column))) {
        errors.add(entry.device + ".s" + column + ": expected INT64 metadata");
      }
    }
    boolean foundDevice = false;
    try (SessionDataSet data = session.executeQueryStatement("SHOW DEVICES " + entry.device)) {
      SessionDataSet.DataIterator iterator = data.iterator();
      while (iterator.next()) {
        if (entry.device.equals(iterator.getString("Device"))) {
          String actual = iterator.getString("IsAligned");
          if (foundDevice || !("true".equalsIgnoreCase(actual) || "false".equalsIgnoreCase(actual))
              || aligned != Boolean.parseBoolean(actual)) {
            errors.add(entry.device + ": alignment metadata mismatch");
          }
          foundDevice = true;
        }
      }
    }
    if (!foundDevice) {
      errors.add(entry.device + ": device metadata missing");
    }
    StringBuilder aggregate = new StringBuilder("SELECT ");
    for (int column = 0; column < 10; column++) {
      if (column > 0) {
        aggregate.append(',');
      }
      aggregate.append("count(s").append(column).append("),min_time(s").append(column)
          .append("),max_time(s").append(column).append(')');
    }
    aggregate.append(" FROM ").append(entry.device);
    try (SessionDataSet data = session.executeQueryStatement(aggregate.toString())) {
      require(data.hasNext(), "Aggregate query returned no row");
      List<Field> fields = data.next().getFields();
      require(fields.size() == 30 && !data.hasNext(), "Aggregate query shape differs from ten series");
      for (int column = 0; column < 10; column++) {
        String series = entry.device + ".s" + column;
        long count = integer(fields.get(3 * column));
        long first = integer(fields.get(3 * column + 1));
        long last = integer(fields.get(3 * column + 2));
        if (count != entry.rows) {
          errors.add(series + ": COUNT expected " + entry.rows + ", found " + count);
        }
        if (first != entry.first || last != entry.last) {
          errors.add(series + ": timestamp bounds expected " + entry.first + ".." + entry.last
              + ", found " + first + ".." + last);
        }
      }
    }
    Set<Long> samples = new LinkedHashSet<>();
    samples.add(entry.first);
    samples.add(entry.first + (entry.rows - 1) / 2);
    samples.add(entry.last);
    for (long timestamp : samples) {
      try (SessionDataSet data = session.executeQueryStatement(
          "SELECT " + NAMES + " FROM " + entry.device + " WHERE time = " + timestamp)) {
        require(data.hasNext(), "Sample query returned no row");
        RowRecord row = data.next();
        require(row.getTimestamp() == timestamp && row.getFields().size() == 10 && !data.hasNext(),
            "Sample query row/column shape mismatch");
        for (int column = 0; column < 10; column++) {
          long expected = timestamp - entry.first + column;
          long actual = integer(row.getFields().get(column));
          if (actual != expected) {
            errors.add(entry.device + ".s" + column + ": timestamp " + timestamp
                + " expected value " + expected + ", found " + actual);
          }
        }
      }
    }
    return errors.size() == initialErrors;
  }

  private static long integer(Field field) {
    require(field != null && field.getDataType() == TSDataType.INT64, "Query returned a null/non-INT64 field");
    return field.getLongV();
  }

  static List<Entry> parseManifest(byte[] raw) throws CharacterCodingException {
    String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString();
    String[] lines = text.split("\n", -1);
    require(HEADER.equals(stripCr(lines[0])), "Manifest must have the exact TSV header");
    List<Entry> entries = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    long totalPoints = 0;
    for (int line = 1; line < lines.length; line++) {
      String value = stripCr(lines[line]);
      if (line == lines.length - 1 && value.isEmpty()) {
        continue;
      }
      String[] fields = value.split("\t", -1);
      require(fields.length == 4, "Manifest line " + (line + 1) + " must have four TSV fields");
      require(fields[0].matches("root(?:\\.[A-Za-z_][A-Za-z0-9_]*)+"),
          "Manifest line " + (line + 1) + " has an invalid device path");
      require(seen.add(fields[0]), "Manifest contains a duplicate device at line " + (line + 1));
      try {
        long rows = unsigned(fields[1]);
        long first = unsigned(fields[2]);
        long last = unsigned(fields[3]);
        require(rows > 0, "Manifest rows must be greater than zero at line " + (line + 1));
        require(Math.addExact(first, rows - 1) == last,
            "Manifest timestamp bounds must describe every row with step 1 at line " + (line + 1));
        totalPoints = Math.addExact(totalPoints, Math.multiplyExact(rows, 10L));
        entries.add(new Entry(fields[0], rows, first, last));
      } catch (NumberFormatException | ArithmeticException error) {
        throw new ValidationFailure("Manifest numeric value is invalid/out of range at line " + (line + 1));
      }
    }
    require(!entries.isEmpty(), "Manifest must contain at least one device");
    return entries;
  }

  private static long unsigned(String value) {
    require(value.matches("[0-9]+"), "Manifest numeric fields must contain unsigned decimal integers");
    return Long.parseLong(value);
  }

  private static String stripCr(String value) {
    return value.endsWith("\r") ? value.substring(0, value.length() - 1) : value;
  }

  static String sha256(byte[] raw) throws Exception {
    StringBuilder hash = new StringBuilder();
    for (byte value : MessageDigest.getInstance("SHA-256").digest(raw)) {
      hash.append(String.format("%02x", value & 0xff));
    }
    return hash.toString();
  }

  static Options parseOptions(String[] args) {
    Map<String, String> values = new HashMap<>();
    for (int index = 0; index < args.length; index += 2) {
      String key = args[index];
      require(key.equals("--manifest") || key.equals("--output") || key.equals("--host")
          || key.equals("--port") || key.equals("--username") || key.equals("--aligned"),
          "Unknown readback option");
      require(index + 1 < args.length && !args[index + 1].isEmpty() && !args[index + 1].startsWith("--"),
          "Readback option requires a value");
      require(values.put(key, args[index + 1]) == null, "Duplicate readback option");
    }
    require(values.containsKey("--manifest") && values.containsKey("--output"),
        "Readback requires --manifest <TSV> --output <JSON>");
    int port;
    try {
      port = Integer.parseInt(values.getOrDefault("--port", "6667"));
    } catch (NumberFormatException error) {
      throw new ValidationFailure("Readback port must be an integer");
    }
    require(port > 0 && port <= 65535, "Readback port must be between 1 and 65535");
    String aligned = values.getOrDefault("--aligned", "false");
    require(aligned.equals("true") || aligned.equals("false"), "Readback aligned must be true or false");
    return new Options(Paths.get(values.get("--manifest")).toAbsolutePath().normalize(),
        Paths.get(values.get("--output")).toAbsolutePath().normalize(),
        values.getOrDefault("--host", "127.0.0.1"), port,
        values.getOrDefault("--username", "root"), Boolean.parseBoolean(aligned));
  }

  private static Path extractPath(String[] args, String flag) {
    for (int index = 0; index + 1 < args.length; index++) {
      if (flag.equals(args[index])) {
        try {
          return Paths.get(args[index + 1]).toAbsolutePath().normalize();
        } catch (RuntimeException ignored) {
          return null;
        }
      }
    }
    return null;
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new ValidationFailure(message);
    }
  }

  static final class Entry {
    final String device;
    final long rows;
    final long first;
    final long last;
    Entry(String device, long rows, long first, long last) {
      this.device = device;
      this.rows = rows;
      this.first = first;
      this.last = last;
    }
  }

  static final class Options {
    final Path manifest;
    final Path output;
    final String host;
    final int port;
    final String username;
    final boolean aligned;
    Options(Path manifest, Path output, String host, int port, String username, boolean aligned) {
      this.manifest = manifest;
      this.output = output;
      this.host = host;
      this.port = port;
      this.username = username;
      this.aligned = aligned;
    }
  }

  static final class Report {
    boolean passed;
    String manifestSha256;
    long verifiedDevices;
    long verifiedPoints;
    String serverVersion;
    final List<String> errors = new ArrayList<>();

    String json() {
      return json(null);
    }

    String json(String secret) {
      StringBuilder json = new StringBuilder("{\n  \"passed\": ").append(passed)
          .append(",\n  \"manifest_sha256\": ").append(quote(manifestSha256))
          .append(",\n  \"verified_devices\": ").append(verifiedDevices)
          .append(",\n  \"verified_points\": ").append(verifiedPoints)
          .append(",\n  \"server_version\": ").append(quote(redact(serverVersion, secret)))
          .append(",\n  \"errors\": [");
      for (int index = 0; index < errors.size(); index++) {
        if (index > 0) {
          json.append(',');
        }
        json.append(quote(redact(errors.get(index), secret)));
      }
      return json.append("]\n}\n").toString();
    }
  }

  private static final class ValidationFailure extends IllegalArgumentException {
    ValidationFailure(String message) {
      super(message);
    }
  }

  private static String redact(String value, String secret) {
    return value != null && secret != null && !secret.isEmpty() ? value.replace(secret, "[redacted]") : value;
  }

  private static String quote(String value) {
    if (value == null) {
      return "null";
    }
    StringBuilder escaped = new StringBuilder("\"");
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (character == '\\' || character == '"') {
        escaped.append('\\').append(character);
      } else if (character < 0x20) {
        escaped.append(String.format("\\u%04x", (int) character));
      } else {
        escaped.append(character);
      }
    }
    return escaped.append('"').toString();
  }
}
