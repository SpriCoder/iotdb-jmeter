package org.apache.iotdb.jmeter.test;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.session.Session;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.util.Arrays;
import java.util.List;

public class JmeterTest extends AbstractJavaSamplerClient {
  private String database;
  private int op_count;

  /**
   * 这个方法用来控制显示在GUI页面的属性，由用户来进行设置。
   * 此方法不用调用，是一个与生命周期相关的方法，类加载则运行。
   */
  public Arguments getDefaultParameters() {
    Arguments arguments = new Arguments();
    arguments.addArgument("op_count", "100");
    arguments.addArgument("database", "database");
    return arguments;
  }

  /**
   * 初始化方法，初始化性能测试时的每个线程
   * 实际运行时每个线程仅执行一次，在测试方法运行前执行，类似于LoadRunner中的init方法
   */
  public void setupTest(JavaSamplerContext jsc){
    database = jsc.getParameter("database");
    op_count = Integer.valueOf(jsc.getParameter("op_count"));
  }

  /**
   * 性能测试时的线程运行体
   * 测试执行的循环体，根据线程数和循环次数的不同可执行多次，类似于Loadrunner中的Action方法
   */
  public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
    SampleResult results = new SampleResult();
    results.setSamplerData(database + "\n"+ op_count);
    // 标记事务开始
    results.sampleStart();
    try {
      getNewLogger().info("parameters: {} {}", database, op_count);
      results.setSuccessful(true);
    } catch (Exception e) {
      results.setSuccessful(false);
      e.printStackTrace();
    }
    // 标记事务结束
    results.sampleEnd();
    return results;
  }

  /**
   * 测试结束方法，结束测试中的每个线程
   * 实际运行时，每个线程仅执行一次，在测试方法运行结束后执行，类似于Loadrunner中的End方法
   */
  public void teardownTest(JavaSamplerContext args) {}
}