package org.apache.iotdb.jmeter.test;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;

/** Reuses bounded column buffers instead of allocating a new Tablet for every request. */
final class TabletFactory {
  private final WriteConfig config;
  private final Tablet tablet;

  TabletFactory(WriteConfig config, String device) {
    this.config = config;
    List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int column = 0; column < config.measurements.size(); column++) {
      schemas.add(new MeasurementSchema(config.measurements.get(column), config.dataTypes.get(column)));
    }
    tablet = new Tablet(device, schemas, config.batchSize);
  }

  Tablet fill(int rows, long firstTimestamp, long firstValue) {
    tablet.reset();
    for (int row = 0; row < rows; row++) {
      tablet.addTimestamp(row, Math.addExact(firstTimestamp, Math.multiplyExact(row, config.timestampStep)));
      long sequence = Math.addExact(firstValue, Math.multiplyExact(row, config.valueStep));
      for (int column = 0; column < config.dataTypes.size(); column++) {
        long value = Math.addExact(sequence, column);
        TSDataType type = config.dataTypes.get(column);
        switch (type) {
          case BOOLEAN: tablet.addValue(row, column, (value & 1L) == 0); break;
          case INT32: tablet.addValue(row, column, Math.toIntExact(value)); break;
          case INT64: tablet.addValue(row, column, value); break;
          case FLOAT: tablet.addValue(row, column, (float) value); break;
          case DOUBLE: tablet.addValue(row, column, (double) value); break;
          case TEXT: case STRING: tablet.addValue(row, column, "v" + value); break;
          default: throw new IllegalArgumentException("Unsupported type: " + type);
        }
      }
    }
    tablet.setRowSize(rows);
    return tablet;
  }

  // Validate the entire sample before submitting a first batch, avoiding partial
  // writes caused by a numeric overflow later in the generated sequence.
  void validateValues(long firstValue, int rows) {
    long lastValue = Math.addExact(firstValue, Math.multiplyExact(rows - 1L, config.valueStep));
    for (int column = 0; column < config.dataTypes.size(); column++) {
      long first = Math.addExact(firstValue, column);
      long last = Math.addExact(lastValue, column);
      if (config.dataTypes.get(column) == TSDataType.INT32) {
        Math.toIntExact(first);
        Math.toIntExact(last);
      }
    }
  }
}
