package org.apache.iotdb.jmeter.test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TimestampGenerator {
  private AtomicLong currentTimestamp;
  private final long interval;
  private final long valueInterval;
  private final TimeUnit timeUnits;


  public TimestampGenerator(final TimeUnit timeUnits, final long startTimestamp) {
    this.interval = 100;
    this.timeUnits = timeUnits;
    this.valueInterval = getOffset(1);
    this.currentTimestamp = new AtomicLong(startTimestamp - getOffset(interval));
  }

  public Long nextValue() {
    return this.currentTimestamp.addAndGet(valueInterval);
  }

  public long getOffset(final long intervalOffset) {
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

}
