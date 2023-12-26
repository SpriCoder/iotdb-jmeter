package org.apache.iotdb.jmeter.test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.iotdb.jmeter.test.IoTDBClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.glassfish.jaxb.core.Utils;

public class ClientThread implements Runnable{
    private final CountDownLatch _completeLatch;
    private String _database;
    private IoTDBClient _db;
    private int _opcount;
    private int _opsdone;

    public ClientThread(String database, int opcount, CountDownLatch completeLatch) {
        _database = database;
        _db= new IoTDBClient();
        _opcount=opcount;
        _opsdone=0;
        _completeLatch=completeLatch;
    }    

    public int getOpsDone() {
      return _opsdone;
    }

    @Override
    public void run() {
        _db.init(_database);

        while ((_opcount == 0) || (_opsdone < _opcount)) {
            _db.insert();
            _opsdone++;
        }
        _db.cleanup();

        _completeLatch.countDown();
    }    
}
