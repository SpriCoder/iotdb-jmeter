package org.apache.iotdb.jmeter.test;

import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;

public class ClientThread implements Runnable {
  private final CountDownLatch _completeLatch;
  private final String _database;
  private final IoTDBClient _db;
  private final int _opcount;
  private int _opsdone;
  private final long _runStartTime;
  private final TimestampGenerator _tt;
  private final String _threadName;
  private final Logger _logger;

  public ClientThread(String database, long runStartTime, int opcount, CountDownLatch completeLatch, TimestampGenerator tt, Logger logger) {
    _database = database;
    _db = new IoTDBClient(logger);
    _runStartTime = runStartTime;
    _opcount = opcount;
    _opsdone = 0;
    _completeLatch = completeLatch;
    _tt = tt;
    _threadName = Thread.currentThread().getName();
    _logger = logger;
  }

  public int getOpsDone() {
    return _opsdone;
  }

  @Override
  public void run() {
    _db.init(_database, _runStartTime, _tt);

    while ((_opcount == 0) || (_opsdone < _opcount)) {
      _db.insert();
      if (_opsdone / 500000 > 0 && _opsdone % 500000 == 0) {
        _logger.info("Thread " + _threadName +
            " inserted " + _opsdone * 100.0 / _opcount + "% records");
      }
      _opsdone++;
    }
    _db.cleanup();

    _completeLatch.countDown();
  }
}
