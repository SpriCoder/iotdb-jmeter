package org.apache.iotdb.jmeter.test;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.ICompressor.IOTDBLZ4Compressor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class IoTDBClient {
  private TimestampGenerator tt;
  private ValueGenerator vg;
  private Session session = null;
  private Map<String, List<Pair<Long, Pair<String, byte[]>>>> cacheData;  //  <device_id,<timestamp,<sensor_id,value>>>
  private int cacheNum = 0;
  private final int FETCH_SIZE = 256;
  private String db_host = "localhost";
  private final int db_port = 6667;
  private String database_id;
  private long runStartTime;
  private final int cache_threshold = 10000;
  private ICompressor compressor = null;
  private Logger logger;

  public IoTDBClient(Logger logger) {
    this.logger = logger;
  }

  public void init(String database, long startTime, TimestampGenerator tg) {
    tt = tg;
    vg = new ValueGenerator();
    database_id = database;
    runStartTime = startTime;
    int instance_id = Integer.parseInt(database_id.substring(13));
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
      logger.error("start session({}:{}) failed:", db_host, db_port, e);
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
      logger.error("insert({}:{}) failed:", db_host, db_port, e);
      return -1;
    }
    return 1;
  }

  private final List<MeasurementSchema> schemaList = Collections.singletonList(
      new MeasurementSchema("field0", TSDataType.TEXT));

  private Map<String, Tablet> generateTablets() {
    Map<String, Tablet> tabletsMap = new HashMap<>();
    for (Map.Entry<String, List<Pair<Long, Pair<String, byte[]>>>> entry : cacheData.entrySet()) {  // create a tablet for each device
      List<Pair<Long, Pair<String, byte[]>>> TVList = entry.getValue();  // TVList是<timestamp, <measurement_id, value>>的三元组构成的list
      Tablet tablet = new Tablet(entry.getKey(), schemaList, TVList.size());
      tabletsMap.put(entry.getKey(), tablet);
      for (Pair<Long, Pair<String, byte[]>> pair : TVList) {
        int row1 = tablet.rowSize++;
        tablet.addTimestamp(row1, pair.left);
        tablet.addValue(schemaList.get(0).getMeasurementId(), row1, new Binary(pair.right.right));
      }
    }
    return tabletsMap;
  }

  public void cleanup() {
    try {
      if (!cacheData.isEmpty()) {
        Map<String, Tablet> tablets = generateTablets();
        session.insertTablets(tablets);
        cacheData.clear();
      }
      session.close();
      session = null;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error("cleanup session({}:{}) failed:", db_host, db_port, e);
    }
  }
}
