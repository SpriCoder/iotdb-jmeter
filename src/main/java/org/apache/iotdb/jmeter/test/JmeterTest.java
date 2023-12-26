package org.apache.iotdb.jmeter.test;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JmeterTest extends AbstractJavaSamplerClient {
  private int threadcount;
  private int opcount;
  private String database;
  private List<ClientThread> clientThreads;
  private CountDownLatch completeLatch;
  private long st;
  private long en;
  private int opsDone;
  private long runStartTime;
  private TimestampGenerator tt;

  /**
   * 这个方法用来控制显示在GUI页面的属性，由用户来进行设置。
   * 此方法不用调用，是一个与生命周期相关的方法，类加载则运行。
   */
  public Arguments getDefaultParameters() {
    Arguments arguments = new Arguments();
    arguments.addArgument("op_count", "127.0.0.1:6667");
    arguments.addArgument("database", "root");
    return arguments;
  }

  /**
   * 初始化方法，初始化性能测试时的每个线程
   * 实际运行时每个线程仅执行一次，在测试方法运行前执行，类似于LoadRunner中的init方法
   */
  public void setupTest(JavaSamplerContext jsc) {
    threadcount = 10;  // fixed
    opcount = jsc.getIntParameter("op_count");  // adjustab e
    database = jsc.getParameter("database");  // adjustable
    clientThreads = new ArrayList<ClientThread>(threadcount);
    completeLatch = new CountDownLatch(threadcount);
    opsDone = 0;
    runStartTime = System.currentTimeMillis();
    tt = new TimestampGenerator(100, TimeUnit.MILLISECONDS, System.currentTimeMillis());
  }

  /**
   * 性能测试时的线程运行体
   * 测试执行的循环体，根据线程数和循环次数的不同可执行多次，类似于Loadrunner中的Action方法
   */
  public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
    SampleResult results = new SampleResult();
    results.setSamplerData(database + "\n" + opcount);
    results.setRequestHeaders(database);
    // 标记事务开始
    results.sampleStart();
    final Map<Thread, ClientThread> threads = new HashMap<Thread, ClientThread>(threadcount);
    try {
      int threadopcount = opcount / threadcount;
      for (int threadid = 0; threadid < threadcount; threadid++) {
        ClientThread t = new ClientThread(database, runStartTime, threadopcount, completeLatch, tt, getNewLogger());
        clientThreads.add(t);
      }

      for (ClientThread client : clientThreads) {
        threads.put(new Thread(client, "ClientThread"), client);
      }
      st = System.currentTimeMillis();
      for (Thread t : threads.keySet()) {
        t.start();
      }
      for (Map.Entry<Thread, ClientThread> entry : threads.entrySet()) {
        entry.getKey().join();
        opsDone += entry.getValue().getOpsDone();
      }
      en = System.currentTimeMillis();
      results.setSuccessful(true);
    } catch (Exception e) {
      results.setSuccessful(false);
      getNewLogger().error("Unexpected error", e);
      for (Map.Entry<Thread, ClientThread> entry : threads.entrySet()) {
        entry.getKey().interrupt();
      }
    }
    // 标记事务结束
    results.setResponseMessage("Database: " + database + "\n" + "Operation Done: " + opsDone + "\n" + "Test Time: " + (en-st));
    results.sampleEnd();
    return results;
  }

  /**
   * 测试结束方法，结束测试中的每个线程
   * 实际运行时，每个线程仅执行一次，在测试方法运行结束后执行，类似于Loadrunner中的End方法
   */
  public void teardownTest(JavaSamplerContext args) {
    // do nothing
  }
}