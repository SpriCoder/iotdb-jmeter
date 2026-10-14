package org.apache.iotdb.jmeter.test;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.*;

public class FixedWorkloadReadbackTest {
  private static final String HEADER = "device\trows\tfirst_timestamp\tlast_timestamp\n";
  @Rule public TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void parsesExactPerDeviceBoundsForFormalAndSmallFixtures() throws Exception {
    List<FixedWorkloadReadback.Entry> entries = parse(HEADER
        + "root.run_t1.d0\t240000\t1700000000000\t1700000239999\n"
        + "root.run_t1.d1\t1\t42\t42\n");
    assertEquals(2, entries.size());
    assertEquals("root.run_t1.d0", entries.get(0).device);
    assertEquals(240000L, entries.get(0).rows);
    assertEquals(1700000000000L, entries.get(0).first);
    assertEquals(1700000239999L, entries.get(0).last);
    assertEquals(1L, entries.get(1).rows);
  }

  @Test
  public void acceptsCrLfButHashesTheExactUnnormalizedFileBytes() throws Exception {
    String lf = HEADER + "root.run.d0\t100\t0\t99\n";
    String crlf = lf.replace("\n", "\r\n");
    assertEquals(100L, parse(crlf).get(0).rows);
    assertNotEquals(FixedWorkloadReadback.sha256(lf.getBytes(StandardCharsets.UTF_8)),
        FixedWorkloadReadback.sha256(crlf.getBytes(StandardCharsets.UTF_8)));
    assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        FixedWorkloadReadback.sha256("abc".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void rejectsDuplicateDevicesAndSqlPathSyntaxBeforeAnyQuery() {
    assertThrows(IllegalArgumentException.class, () -> parse(HEADER
        + "root.run.d0\t100\t0\t99\nroot.run.d0\t100\t100\t199\n"));
    for (String device : new String[] {"root.run.*", "root.run.**", "root.run.`d0`",
        "root.run.d0;DELETE", "other.run.d0", "root", "root.run.0", "root.run.d0 "}) {
      assertThrows(device, IllegalArgumentException.class,
          () -> parse(HEADER + device + "\t100\t0\t99\n"));
    }
  }

  @Test
  public void rejectsEmptyOrMalformedTsvAndNonUtf8() {
    for (String input : new String[] {"", HEADER, "device,rows,first_timestamp,last_timestamp\n",
        HEADER + "root.run.d0\t100\t0\t99\textra\n",
        HEADER + "root.run.d0\t100\t0\n", HEADER + "\n"}) {
      assertThrows(IllegalArgumentException.class, () -> parse(input));
    }
    assertThrows(CharacterCodingException.class,
        () -> FixedWorkloadReadback.parseManifest(new byte[] {(byte) 0xc3, (byte) 0x28}));
  }

  @Test
  public void rejectsNonpositiveRowsGapsUnsignedFormatAndOverflow() {
    for (String numeric : new String[] {"0\t0\t0", "-1\t0\t0", "+1\t0\t0",
        "1 \t0\t0", "100\t0\t100", "100\t10\t99", "1\t-1\t-1",
        "2\t9223372036854775807\t9223372036854775807",
        "922337203685477581\t0\t922337203685477580",
        "9223372036854775808\t0\t0"}) {
      assertThrows(numeric, IllegalArgumentException.class,
          () -> parse(HEADER + "root.run.d0\t" + numeric + "\n"));
    }
  }

  @Test
  public void validatesCliWithoutAcceptingPasswordOrDuplicateOptions() {
    FixedWorkloadReadback.Options options = FixedWorkloadReadback.parseOptions(new String[] {
        "--manifest", "sources.tsv", "--output", "readback.json", "--host", "localhost",
        "--port", "6667", "--username", "reader", "--aligned", "true"});
    assertEquals("localhost", options.host);
    assertEquals(6667, options.port);
    assertEquals("reader", options.username);
    assertTrue(options.aligned);
    assertTrue(options.manifest.isAbsolute());
    for (String[] args : new String[][] {
        {"--manifest", "sources.tsv"},
        {"--manifest", "sources.tsv", "--output", "result.json", "--password", "secret"},
        {"--manifest", "sources.tsv", "--output", "result.json", "--port", "0"},
        {"--manifest", "sources.tsv", "--output", "result.json", "--aligned", "unknown"},
        {"--manifest", "sources.tsv", "--output", "result.json", "--port", "6667", "--port", "6668"},
        {"--manifest", "sources.tsv", "--output", "--host"}}) {
      assertThrows(IllegalArgumentException.class, () -> FixedWorkloadReadback.parseOptions(args));
    }
  }

  @Test
  public void malformedManifestStillWritesFailureJsonWithExactHashAndNoNetwork() throws Exception {
    Path manifest = temporary.newFile("invalid.tsv").toPath();
    Path output = temporary.getRoot().toPath().resolve("result.json");
    byte[] raw = "not a manifest\n".getBytes(StandardCharsets.UTF_8);
    Files.write(manifest, raw);
    assertEquals(1, FixedWorkloadReadback.run(new String[] {
        "--manifest", manifest.toString(), "--output", output.toString()}));
    String json = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
    assertTrue(json.contains("\"passed\": false"));
    assertTrue(json.contains(FixedWorkloadReadback.sha256(raw)));
    assertTrue(json.contains("\"verified_devices\": 0"));
    assertTrue(json.contains("\"server_version\": null"));
  }

  @Test
  public void rejectsOutputReplacingTheManifest() throws Exception {
    Path manifest = temporary.newFile("sources.tsv").toPath();
    byte[] raw = (HEADER + "root.run.d0\t1\t0\t0\n").getBytes(StandardCharsets.UTF_8);
    Files.write(manifest, raw);
    assertEquals(2, FixedWorkloadReadback.run(new String[] {
        "--manifest", manifest.toString(), "--output", manifest.toString()}));
    assertArrayEquals(raw, Files.readAllBytes(manifest));
    assertEquals(2, FixedWorkloadReadback.run(new String[] {
        "--manifest", manifest.toString(), "--output", manifest.toString(), "--port", "bad"}));
    assertArrayEquals(raw, Files.readAllBytes(manifest));
    assertEquals(2, FixedWorkloadReadback.run(new String[] {
        "--manifest", manifest.toString(), "--output", manifest.toString(), "--unknown", "value"}));
    assertArrayEquals(raw, Files.readAllBytes(manifest));
  }

  @Test
  public void escapesAndRedactsOnlyJsonStringsWithoutChangingBooleanOrHash() {
    FixedWorkloadReadback.Report report = new FixedWorkloadReadback.Report();
    report.manifestSha256 = "abcdef";
    report.serverVersion = "version false";
    report.errors.add("error \"false\"\n\\tail");
    String json = report.json("false");
    assertTrue(json.contains("\"passed\": false"));
    assertTrue(json.contains("\"manifest_sha256\": \"abcdef\""));
    assertTrue(json.contains("version [redacted]"));
    assertTrue(json.contains("error \\\"[redacted]\\\"\\u000a\\\\tail"));
    assertFalse(json.contains("version false"));
  }

  private static List<FixedWorkloadReadback.Entry> parse(String text) throws CharacterCodingException {
    return FixedWorkloadReadback.parseManifest(text.getBytes(StandardCharsets.UTF_8));
  }
}
