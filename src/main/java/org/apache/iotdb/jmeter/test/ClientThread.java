package org.apache.iotdb.jmeter.test;

import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;

public class ClientThread implements Runnable{
    private final CountDownLatch _completeLatch;
    private String _database;
    private IoTDBClient _db;
    private int _opcount;
    private int _opsdone;
    private long _runStartTime;
    private TimestampGenerator _tt;
    private Logger _logger;

    public ClientThread(String database, long runStartTime, int opcount, CountDownLatch completeLatch, TimestampGenerator tt, Logger logger) {
        _database = database;
        _db= new IoTDBClient();
        _runStartTime = runStartTime;
        _opcount=opcount;
        _opsdone=0;
        _completeLatch=completeLatch;
        _tt = tt;
        _logger= logger;
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
                _logger.info("Thread " + Thread.currentThread().getId() +
                    " inserted " + _opsdone * 100.0 / _opcount + "% records");
            }
            _opsdone++;
        }
        _db.cleanup();

        _completeLatch.countDown();
    }    
}
