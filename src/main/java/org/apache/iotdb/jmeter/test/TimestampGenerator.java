package org.apache.iotdb.jmeter.test;

import java.util.concurrent.TimeUnit;

public class TimestampGenerator {
  private long currentTimestamp;
  private long lastTimestamp;
  private long interval;
  private TimeUnit timeUnits;


  public TimestampGenerator(final long interval, final TimeUnit timeUnits, final long startTimestamp) {
    this.interval = interval;
    this.timeUnits = timeUnits;
    this.currentTimestamp = startTimestamp - getOffset(interval);
    lastTimestamp = currentTimestamp - getOffset(interval);
  }

  public void initalizeTimestamp(final long intervalOffset) {
    switch (timeUnits) {
    case NANOSECONDS:
      currentTimestamp = System.nanoTime() + getOffset(intervalOffset);
      break;
    case MICROSECONDS:
      currentTimestamp = (System.nanoTime() / 1000) + getOffset(intervalOffset);
      break;
    case MILLISECONDS:
      currentTimestamp = System.currentTimeMillis() + getOffset(intervalOffset);
      break;
    case SECONDS:
      currentTimestamp = (System.currentTimeMillis() / 1000) + 
          getOffset(intervalOffset);
      break;
    case MINUTES:
      currentTimestamp = (System.currentTimeMillis() / 1000) + 
          getOffset(intervalOffset);
      break;
    case HOURS:
      currentTimestamp = (System.currentTimeMillis() / 1000) + 
          getOffset(intervalOffset);
      break;
    case DAYS:
      currentTimestamp = (System.currentTimeMillis() / 1000) + 
          getOffset(intervalOffset);
      break;
    default:
      throw new IllegalArgumentException("Unhandled time unit type: " + timeUnits);
    }
  }
  
  public Long nextValue() {
    lastTimestamp = currentTimestamp;
    currentTimestamp += getOffset(1);
    return currentTimestamp;
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

  public Long lastValue() {
    return lastTimestamp;
  }

  public long currentValue() {
    return currentTimestamp;
  }

}
