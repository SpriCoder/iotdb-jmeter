package org.apache.iotdb.jmeter.test;

import org.apache.jmeter.samplers.SampleResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** One JVM, one common start after every worker has prepared and warmed its sources. */
final class FixedRunCoordinator {
  private static final Map<String, FixedRunCoordinator> RUNS = new HashMap<>();

  static synchronized FixedRunCoordinator acquire(String runId, String signature, int workers) {
    FixedRunCoordinator run = RUNS.get(runId);
    if (run == null) {
      run = new FixedRunCoordinator(runId, signature, workers);
      RUNS.put(runId, run);
    } else if (!run.signature.equals(signature)) {
      run.fail();
      throw new IllegalArgumentException("Workers in a run must use identical workload settings");
    }
    return run;
  }

  static synchronized void failIfPresent(String runId) {
    FixedRunCoordinator run = RUNS.get(runId);
    if (run != null) {
      run.fail();
    }
  }

  private final String runId;
  private final String signature;
  private final boolean[] ready;
  private int registered;
  private int released;
  private boolean failed;
  private long startMillis;

  private FixedRunCoordinator(String runId, String signature, int workers) {
    this.runId = runId;
    this.signature = signature;
    this.ready = new boolean[workers];
  }

  synchronized int register() {
    if (failed || registered == ready.length) {
      fail();
      throw new IllegalStateException("Too many workers or run preparation already failed");
    }
    return registered++;
  }

  synchronized long awaitStart(int worker, long timeoutMillis) throws InterruptedException {
    if (ready[worker]) {
      throw new IllegalStateException("Worker was prepared twice");
    }
    ready[worker] = true;
    boolean allReady = registered == ready.length;
    for (boolean item : ready) {
      allReady &= item;
    }
    if (allReady && !failed) {
      // Use the same wall-clock conversion as JTL sample timestamps.
      startMillis = new SampleResult().currentTimeInMillis();
      notifyAll();
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (startMillis == 0 && !failed) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        fail();
        throw new IllegalStateException("Timed out waiting for all workers to prepare");
      }
      try {
        TimeUnit.NANOSECONDS.timedWait(this, remaining);
      } catch (InterruptedException error) {
        fail();
        Thread.currentThread().interrupt();
        throw error;
      }
    }
    return requireStarted();
  }

  synchronized long requireStarted() {
    if (failed || startMillis == 0) {
      throw new IllegalStateException("Fixed workload has not started or a worker failed");
    }
    return startMillis;
  }

  synchronized void fail() {
    failed = true;
    notifyAll();
  }

  void release() {
    boolean finished;
    synchronized (this) {
      finished = ++released == registered;
    }
    if (finished) {
      synchronized (FixedRunCoordinator.class) {
        if (RUNS.get(runId) == this) {
          RUNS.remove(runId);
        }
      }
    }
  }
}
