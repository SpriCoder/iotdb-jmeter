package org.apache.iotdb.jmeter.test;

import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class TabletFactoryTest {
  @Test
  public void fillsEverySupportedTypeAndResetsPartialBatch() {
    WriteConfig config = new WriteConfig(JmeterTestTest.context(
        "measurements", "a,b,c,d,e,f,g",
        "data_types", "BOOLEAN,INT32,INT64,FLOAT,DOUBLE,TEXT,STRING",
        "batch_size", "5", "timestamp_step_ms", "10", "value_step", "2"));
    TabletFactory factory = new TabletFactory(config, "root.test.device");
    Tablet full = factory.fill(5, 100, 0);
    assertEquals(5, full.getRowSize());
    assertEquals(140, full.getTimestamp(4));
    assertEquals(true, full.getValue(4, 0));
    assertEquals(9, full.getValue(4, 1));
    assertEquals(10L, full.getValue(4, 2));
    assertEquals(11.0f, full.getValue(4, 3));
    assertEquals(12.0, full.getValue(4, 4));
    assertEquals("v13", ((org.apache.tsfile.utils.Binary) full.getValue(4, 5)).getStringValue(StandardCharsets.UTF_8));
    assertEquals("v14", ((org.apache.tsfile.utils.Binary) full.getValue(4, 6)).getStringValue(StandardCharsets.UTF_8));
    Tablet tail = factory.fill(1, 150, 10);
    assertSame(full, tail);
    assertEquals(1, tail.getRowSize());
    assertEquals(150, tail.getTimestamp(0));
    assertEquals(12L, tail.getValue(0, 2));
    assertFalse(tail.isNull(0, 6));
  }

  @Test
  public void acceptsLegacyParameterAliasesWithoutNewNames() {
    org.apache.jmeter.config.Arguments args = new org.apache.jmeter.config.Arguments();
    args.addArgument("password", "test_password_placeholder");
    args.addArgument("database", "root.legacy.device");
    args.addArgument("measurement", "old_value");
    WriteConfig config = new WriteConfig(new org.apache.jmeter.protocol.java.sampler.JavaSamplerContext(args));
    assertEquals("root.legacy.device", config.devicePath);
    assertEquals("old_value", config.measurements.get(0));
    assertEquals("127.0.0.1", config.host);
    assertEquals(6667, config.port);
  }
}
