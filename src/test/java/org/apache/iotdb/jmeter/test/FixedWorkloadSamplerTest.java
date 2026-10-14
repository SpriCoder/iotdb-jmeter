package org.apache.iotdb.jmeter.test;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FixedWorkloadSamplerTest {
  @Test
  public void warmsSeparatePathsAndWritesRoundRobinWithOneConnection() {
    FakeClient client = new FakeClient();
    FixedWorkloadSampler sampler = sampler(client);
    JavaSamplerContext context = context(1, 2, 2);
    sampler.setupTest(context);
    SampleResult prepare = sampler.runTest(context);
    assertTrue(prepare.getResponseMessage(), prepare.isSuccessful());
    assertTrue(prepare.getResponseDataAsString().contains("\"phase\":\"prepare\""));
    assertTrue(prepare.getResponseDataAsString().contains("\"acknowledged_points\":0"));
    assertEquals(1, client.opens);
    assertEquals(4, client.schemas.size());
    assertEquals(2, client.paths.size());
    assertTrue(client.paths.get(0).endsWith(".warmup_0"));
    assertTrue(client.paths.get(1).endsWith(".warmup_1"));
    for (int call = 0; call < 4; call++) {
      SampleResult result = sampler.runTest(context);
      assertTrue(result.getResponseMessage(), result.isSuccessful());
      assertEquals(1, result.getSampleCount());
      assertTrue(result.getResponseDataAsString().contains("\"acknowledged_points\":1000"));
      int write = call + 2;
      assertTrue(client.paths.get(write).endsWith(".source_" + call % 2));
      assertEquals(100 + (call / 2) * 100L, (long) client.firstTimestamps.get(write));
      assertEquals((call / 2) * 100L, (long) client.firstValues.get(write));
      assertEquals(100, (int) client.rows.get(write));
    }
    int calls = client.paths.size();
    SampleResult overQuota = sampler.runTest(context);
    assertFalse(overQuota.isSuccessful());
    assertTrue(overQuota.isStopTest());
    assertEquals(calls, client.paths.size());
    sampler.teardownTest(context);
    assertEquals(1, client.closes);
  }

  @Test
  public void commonStartWaitsForTheSlowestPreparation() throws Exception {
    String runId = unique();
    FixedRunCoordinator run = FixedRunCoordinator.acquire(runId, "same", 2);
    int first = run.register();
    int second = run.register();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    try {
      Future<Long> waiting = executor.submit(() -> {
        entered.countDown();
        return run.awaitStart(first, 1000);
      });
      assertTrue(entered.await(1, TimeUnit.SECONDS));
      assertFalse(waiting.isDone());
      long sharedStart = run.awaitStart(second, 1000);
      assertTrue(sharedStart > 0);
      assertEquals(sharedStart, (long) waiting.get(1, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
      run.release();
      run.release();
    }
  }

  @Test
  public void preparationFailureOrMissingWorkerPreventsFormalWrites() {
    FakeClient client = new FakeClient();
    FixedWorkloadSampler sampler = sampler(client);
    JavaSamplerContext context = context(2, 1, 2, "prepare_timeout_ms", "20");
    sampler.setupTest(context);
    SampleResult result = sampler.runTest(context);
    assertFalse(result.isSuccessful());
    assertTrue(result.isStopTest());
    assertEquals(1, client.paths.size());
    assertTrue(client.paths.get(0).contains("warmup"));
    sampler.teardownTest(context);

    FakeClient dirty = new FakeClient();
    dirty.notEmpty = true;
    FixedWorkloadSampler dirtySampler = sampler(dirty);
    JavaSamplerContext dirtyContext = context(1, 1, 2);
    dirtySampler.setupTest(dirtyContext);
    assertFalse(dirtySampler.runTest(dirtyContext).isSuccessful());
    assertEquals(0, dirty.paths.size());
    dirtySampler.teardownTest(dirtyContext);
  }

  @Test
  public void lostAcknowledgementAbortsWithoutReplayOrCountingPoints() {
    FakeClient client = new FakeClient();
    FixedWorkloadSampler sampler = sampler(client);
    JavaSamplerContext context = context(1, 1, 2);
    sampler.setupTest(context);
    assertTrue(sampler.runTest(context).isSuccessful());
    client.failInsert = 2; // First insertion is warm-up.
    SampleResult failed = sampler.runTest(context);
    assertFalse(failed.isSuccessful());
    assertTrue(failed.isStopTest());
    assertTrue(failed.getResponseDataAsString().contains("\"attempted_rows\":100"));
    assertTrue(failed.getResponseDataAsString().contains("\"acknowledged_points\":0"));
    assertFalse(failed.getResponseMessage().contains("test_password_placeholder"));
    assertFalse(sampler.runTest(context).isSuccessful());
    assertEquals(2, client.paths.size());
    sampler.teardownTest(context);
  }

  @Test
  public void mismatchedWorkerSettingsFailTheCohort() throws Exception {
    String runId = unique();
    FixedRunCoordinator first = FixedRunCoordinator.acquire(runId, "first", 1);
    first.register();
    try {
      FixedRunCoordinator.acquire(runId, "different", 1);
      fail("Mismatched workload should be rejected");
    } catch (IllegalArgumentException expected) {
      try {
        first.awaitStart(0, 20);
        fail("Failed cohort must not start");
      } catch (IllegalStateException failedCohort) {
        assertTrue(failedCohort.getMessage().contains("failed"));
      }
    } finally {
      first.release();
    }
  }

  @Test
  public void invalidWorkerConfigurationWakesAnAlreadyWaitingWorker() throws Exception {
    String runId = unique();
    FakeClient firstClient = new FakeClient();
    FixedWorkloadSampler first = sampler(firstClient);
    JavaSamplerContext firstContext = context(2, 1, 2, "run_id", runId,
        "prepare_timeout_ms", "5000");
    first.setupTest(firstContext);
    FixedWorkloadSampler bad = sampler(new FakeClient());
    JavaSamplerContext badContext = context(2, 1, 2, "run_id", runId, "op_count", "10");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<SampleResult> waiting = executor.submit(() -> first.runTest(firstContext));
      bad.setupTest(badContext);
      assertFalse(bad.runTest(badContext).isSuccessful());
      assertFalse(waiting.get(1, TimeUnit.SECONDS).isSuccessful());
      assertEquals(1, firstClient.paths.size()); // Only warm-up, never a formal write.
    } finally {
      executor.shutdownNow();
      first.teardownTest(firstContext);
      bad.teardownTest(badContext);
    }
  }

  @Test
  public void rejectsNonfixedBatchConfigurationBeforeConnecting() {
    FakeClient client = new FakeClient();
    FixedWorkloadSampler sampler = sampler(client);
    JavaSamplerContext context = context(1, 1, 2, "op_count", "10");
    sampler.setupTest(context);
    assertFalse(sampler.runTest(context).isSuccessful());
    assertEquals(0, client.opens);
    sampler.teardownTest(context);
  }

  private static String unique() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  private static JavaSamplerContext context(int workers, int sources, int batches, String... overrides) {
    Arguments args = new FixedWorkloadSampler().getDefaultParameters();
    String[] base = {"password", "test_password_placeholder", "run_id", unique(),
        "start_timestamp", "100", "workers", Integer.toString(workers),
        "sources_per_worker", Integer.toString(sources), "batches_per_source", Integer.toString(batches)};
    for (String[] settings : new String[][] {base, overrides}) {
      for (int i = 0; i < settings.length; i += 2) {
        args.removeArgument(settings[i]);
        args.addArgument(settings[i], settings[i + 1]);
      }
    }
    return new JavaSamplerContext(args);
  }

  private static FixedWorkloadSampler sampler(FakeClient client) {
    return new FixedWorkloadSampler() {
      @Override WriteClient createClient() { return client; }
    };
  }

  private static final class FakeClient implements WriteClient {
    int opens;
    int closes;
    int failInsert = -1;
    boolean notEmpty;
    final List<String> schemas = new ArrayList<>();
    final List<String> paths = new ArrayList<>();
    final List<Integer> rows = new ArrayList<>();
    final List<Long> firstTimestamps = new ArrayList<>();
    final List<Long> firstValues = new ArrayList<>();
    @Override public void open(WriteConfig config) { opens++; }
    @Override public void prepare(WriteConfig config, String device) { schemas.add(device); }
    @Override public void requireEmpty(String device) {
      if (notEmpty) { throw new IllegalArgumentException("Existing data"); }
    }
    @Override public void insert(Tablet tablet, boolean aligned) throws Exception {
      paths.add(tablet.getDeviceId());
      rows.add(tablet.getRowSize());
      firstTimestamps.add(tablet.getTimestamp(0));
      firstValues.add((Long) tablet.getValue(0, 0));
      if (paths.size() == failInsert) {
        throw new Exception("lost response test_password_placeholder");
      }
    }
    @Override public void close() { closes++; }
  }
}
