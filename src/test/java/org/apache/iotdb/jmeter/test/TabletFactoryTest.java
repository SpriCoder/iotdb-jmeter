package org.apache.iotdb.jmeter.test;

import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.utils.Binary;
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

  @Test
  public void createsTableModelTabletWithSourceTagAndMeasurementFields() {
    WriteConfig config = new WriteConfig(JmeterTestTest.context(
        "model", "table", "database", "nx08_functional", "table_name", "jmeter_probe",
        "source_column", "device_id", "source_id", "jmeter_source",
        "measurements", "s0,s1", "data_types", "INT64,STRING", "batch_size", "2"));
    Tablet tablet = new TabletFactory(config, "jmeter_source_run_probe").fill(2, 100, 0);

    assertEquals("jmeter_probe", tablet.getTableName());
    assertEquals(2, tablet.getRowSize());
    assertEquals(java.util.Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
        tablet.getColumnTypes());
    assertEquals("jmeter_source_run_probe",
        ((Binary) tablet.getValue(0, 0)).getStringValue(StandardCharsets.UTF_8));
    assertEquals(0L, tablet.getValue(0, 1));
    assertEquals("v1", ((Binary) tablet.getValue(0, 2)).getStringValue(StandardCharsets.UTF_8));
    assertEquals(1L, tablet.getValue(1, 1));
    assertEquals("v2", ((Binary) tablet.getValue(1, 2)).getStringValue(StandardCharsets.UTF_8));
  }

  @Test
  public void validatesTableModelIdentifiersAndRejectsTreeAlignment() {
    WriteConfig config = new WriteConfig(JmeterTestTest.context(
        "model", "table", "database", "nx08_functional", "table_name", "jmeter_probe",
        "source_id", "jmeter_source"));
    assertEquals("nx08_functional", config.tableDatabase);
    assertEquals("device_id", config.sourceColumn);

    for (String[] invalid : new String[][] {
        {"database", "nx08;DROP"}, {"table_name", "table.name"},
        {"source_column", "device id"}, {"aligned", "true"}
    }) {
      try {
        new WriteConfig(JmeterTestTest.context("model", "table", "database", "nx08_functional",
            "table_name", "jmeter_probe", "source_id", "jmeter_source",
            invalid[0], invalid[1]));
        fail("Expected invalid table setting: " + invalid[0]);
      } catch (IllegalArgumentException expected) {
        // Validation happens before any database client is opened.
      }
    }
  }
}
