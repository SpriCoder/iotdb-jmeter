package org.apache.iotdb.jmeter.test;

import org.apache.tsfile.write.record.Tablet;

/** Small boundary around network I/O, also used to verify failures without a server. */
interface WriteClient {
  void open(WriteConfig config) throws Exception;
  void prepare(WriteConfig config, String device) throws Exception;
  void insert(Tablet tablet, boolean aligned) throws Exception;
  void close() throws Exception;
}
