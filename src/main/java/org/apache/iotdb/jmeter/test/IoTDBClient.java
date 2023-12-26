package org.apache.iotdb.jmeter.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.iotdb.jmeter.test.TimestampGenerator;
import org.apache.iotdb.jmeter.test.ValueGenerator;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.iotdb.tsfile.compress.ICompressor.IOTDBLZ4Compressor;

public class IoTDBClient {
    protected TimestampGenerator tt;
    protected ValueGenerator vg;
    private Session session = null;
    private static Map<String, List<Pair<Long, Pair<String, byte[]>>>> cacheData;  //  <device_id,<timestamp,<sensor_id,value>>>
    private static int cacheNum = 0;
    private static final int FETCH_SIZE = 256;
    private String db_host = "localhost";
    private int db_port = 6667;
    private String database_id;
    private int cache_threshold = 10000;
    private ICompressor compressor = null;


  public void init(String database) {
    tt = new TimestampGenerator(100,TimeUnit.MILLISECONDS, System.currentTimeMillis());
    database_id = database;
    int instance_id = Integer.parseInt(database_id.substring(13, database_id.length()));
    if (instance_id % 2 == 0) {
        this.db_host = "172.16.17.15";
    } else {
        this.db_host = "172.16.17.16";
    }
    try {
        session = new Session.Builder().host(this.db_host).port(this.db_port).build();
        session.open();
        compressor = new IOTDBLZ4Compressor();
        if (cacheData == null) {
            cacheData = new HashMap<>();
        }
    } catch (IoTDBConnectionException e) {
        System.err.println(
                String.format("start session(%s:%s) failed:%s", db_host, db_port, e.toString()));
        e.printStackTrace();
        // throw new DBException(e);
    }
  }

  public int insert() {
    try {
        long timestamp = tt.nextValue();
        String device_id = String.format("root.%s.%s", database_id, vg.get_device());
        byte[] compressed = compressor.compress(vg.get_value(device_id + timestamp).getBytes());
        Map<String, Tablet> tablets = null;
        cacheData.computeIfAbsent(device_id, k -> new ArrayList<>())
                .add(new Pair<>(timestamp, new Pair<>("field0", compressed)));
        cacheNum++;
        if (cacheNum >= cache_threshold) {
            tablets = generateTablets();
            cacheNum = 0;
            cacheData.clear();
        }
        if (tablets != null) {
            session.insertTablets(tablets);
        }
    } catch (IoTDBConnectionException | StatementExecutionException | IOException e) {
        e.printStackTrace();
        return -1;
    }
    return 1;
  }

  private Map<String, Tablet> generateTablets() {
    List<MeasurementSchema> schemaList = Collections.singletonList(
            new MeasurementSchema("field0", TSDataType.TEXT));
    Map<String, Tablet> tabletsMap = new HashMap<>();
    for (Map.Entry<String, List<Pair<Long, Pair<String, byte[]>>>> entry: cacheData.entrySet()) {  // create a tablet for each device
        List<Pair<Long, Pair<String, byte[]>>> TVList = entry.getValue();  // TVList是<timestamp, <measurement_id, value>>的三元组构成的list
        Tablet tablet = new Tablet(entry.getKey(), schemaList, TVList.size());
        tabletsMap.put(entry.getKey(), tablet);
        for (Pair<Long, Pair<String, byte[]>> pair: TVList) {
            int row1 = tablet.rowSize++;
            tablet.addTimestamp(row1, pair.left);
            tablet.addValue(schemaList.get(0).getMeasurementId(), row1, new Binary(pair.right.right));
        }
    }
    return tabletsMap;
  }

  public void cleanup() {
    try {
        if (cacheData.size() > 0) {
            Map<String, Tablet> tablets = generateTablets();
            session.insertTablets(tablets);
            cacheData.clear();
        }
        session.close();
        session = null;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
        System.err.println(String.format("cleanup session(%s:%s) failed:%s", db_host, db_port, e.toString()));
        e.printStackTrace();
        // throw new DBException();
    }
  }
}
