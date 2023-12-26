package org.apache.iotdb.jmeter.test;

import java.util.concurrent.TimeUnit;

public class TimestampGenerator {
  private long currentTimestamp;
  private long lastTimestamp;
  private final long interval;
  private final TimeUnit timeUnits;


  public TimestampGenerator(final long interval, final TimeUnit timeUnits, final long startTimestamp) {
    this.interval = interval;
    this.timeUnits = timeUnits;
    this.currentTimestamp = startTimestamp - getOffset(interval);
    lastTimestamp = currentTimestamp - getOffset(interval);
  }

  public synchronized Long nextValue() {
    lastTimestamp = currentTimestamp;
    currentTimestamp += getOffset(1);
    return currentTimestamp;
  }

  public synchronized long getOffset(final long intervalOffset) {
    switch (timeUnits) {
      case NANOSECONDS:
      case MICROSECONDS:
      case MILLISECONDS:
      case SECONDS:
        return intervalOffset * interval;
      case MINUTES:
        return intervalOffset * interval * (long) 60;
      case HOURS:
        return intervalOffset * interval * (long) (60 * 60);
      case DAYS:
        return intervalOffset * interval * (long) (60 * 60 * 24);
      default:
        throw new IllegalArgumentException("Unhandled time unit type: " + timeUnits);
    }
  }

  public synchronized Long lastValue() {
    return lastTimestamp;
  }

  public synchronized long currentValue() {
    return currentTimestamp;
  }

}
