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
  private List<String> url;
  private String username;
  private String password;
  private Session session;

  /**
   * 这个方法用来控制显示在GUI页面的属性，由用户来进行设置。
   * 此方法不用调用，是一个与生命周期相关的方法，类加载则运行。
   */
  public Arguments getDefaultParameters() {
    Arguments arguments = new Arguments();
    arguments.addArgument("Url", "127.0.0.1:6667");
    arguments.addArgument("Username", "root");
    arguments.addArgument("Password", "root");
    return arguments;
  }

  /**
   * 初始化方法，初始化性能测试时的每个线程
   * 实际运行时每个线程仅执行一次，在测试方法运行前执行，类似于LoadRunner中的init方法
   */
  public void setupTest(JavaSamplerContext jsc){
    url = Arrays.asList(jsc.getParameter("Url").split(","));
    username = jsc.getParameter("Method");
    username = jsc.getParameter("Username");
    password = jsc.getParameter("Password");
//    session = new Session.Builder()
//        .nodeUrls(url)
//        .username(username)
//        .password(password)
//        .enableRedirection(true)
//        .version(Version.V_1_0)
//        .build();
//    // 打开 Session 连接
//    try {
//      session.open();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
  }

  /**
   * 性能测试时的线程运行体
   * 测试执行的循环体，根据线程数和循环次数的不同可执行多次，类似于Loadrunner中的Action方法
   */
  public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
    SampleResult results = new SampleResult();
    results.setSamplerData(url+"\n"+ username +"\n"+ password);
    results.setRequestHeaders(password);
    //标记事务开始
    results.sampleStart();
    try {
      getNewLogger().info("hello world");
      results.setSuccessful(true);
    } catch (Exception e) {
      results.setSuccessful(false);
      e.printStackTrace();
    }
    //标记事务结束
    results.sampleEnd();
    return results;
  }

  /**
   * 测试结束方法，结束测试中的每个线程
   * 实际运行时，每个线程仅执行一次，在测试方法运行结束后执行，类似于Loadrunner中的End方法
   */
  public void teardownTest(JavaSamplerContext args) {
    // 关闭 Session 连接
//    try {
//      session.close();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
  }
}