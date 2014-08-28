package org.cloudera.mrbenchmark;

import java.io.File;
import java.io.IOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapOutputCollector;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.YarnOutputFiles;
//import org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.mockito.Mockito;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

@SuppressWarnings("rawtypes")
public class MapOutputCollectorBenchmark {

  private static final int NUM_RECORDS = 2500 * 1000;
  private static final int NUM_PARTITIONS = 100;

  /**
   * Fast random number generator.
   */
  static class XorshiftRandom {
    public XorshiftRandom(long seed) {
      x = seed;
    }
    
    public long randomLong() {
      x ^= (x << 21);
      x ^= (x >>> 35);
      x ^= (x << 4);
      return x;
    }
    
    public void nextBytes(byte[] b) {
      int i = 0;
      while (i < b.length - 8) {
        long v = randomLong();
        b[i] = (byte) (v & 0xffL); v <<= 8; i++;
        b[i] = (byte) (v & 0xffL); v <<= 8; i++;
        b[i] = (byte) (v & 0xffL); v <<= 8; i++;
        b[i] = (byte) (v & 0xffL); v <<= 8; i++;
        b[i] = (byte) (v & 0xffL); v <<= 8; i++;
        b[i] = (byte) (v & 0xffL); v <<= 8; i++;
        b[i] = (byte) (v & 0xffL); v <<= 8; i++;
        b[i] = (byte) (v & 0xffL); i++;
      }
      long v = randomLong();
      while (i < b.length) {
        b[i] = (byte) (v & 0xffL); v <<= 8; i++;
      }
    }
    
    long x;
  }

  /**
   * Stopwatch-like class to see how much time is spent for each collector.
   */
  static class ResourceTimer {
    long startCpu;
    long startThisThreadCpu;
    long startCompilation;
    long startWall;
    long startGC;

    ResourceTimer() {
      startCpu = getCpuMillis();
      startThisThreadCpu = getThisThreadCpu();
      startCompilation = getCompilationTime();
      startWall = getWallTime();
      startGC = getGCCpu();
    }

    long elapsedCpu() {
      return getCpuMillis() - startCpu;
    }

    long elapsedCpuThisThread() {
      return getThisThreadCpu() - startThisThreadCpu;
    }

    long elapsedCompilation() {
      return getCompilationTime() - startCompilation;
    }

    long elapsedWall() {
      return getWallTime() - startWall;
    }

    long elapsedGC() {
      return getGCCpu() - startGC;
    }

    long getCompilationTime() {
      CompilationMXBean bean = ManagementFactory.getCompilationMXBean();
      return bean.getTotalCompilationTime();
    }

    long getThisThreadCpu() {
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      return bean.getCurrentThreadCpuTime() / 1000000;
    }

    long getGCCpu() {
      long ret = 0;
      for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
        ret += bean.getCollectionTime();
      }
      return ret;
    }

    long getCpuMillis() {
      String stat = null;
      try {
        stat = Files.readFirstLine(new File("/proc/self/stat"), Charsets.US_ASCII);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      long utime = Long.parseLong(stat.split(" ")[13]);
      return utime * ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS;
    }

    long getWallTime() {
      return System.nanoTime() / 1000000;
    }

  }

  private void doBenchmark(Class<? extends MapOutputCollector> collectorClazz)
    throws Exception {
    JobConf jobConf = new JobConf();
    jobConf.setInt("io.sort.mb", 600);
    jobConf.setInt("io.file.buffer.size", 128*1024);
    jobConf.setMapOutputKeyClass(BytesWritable.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);
    jobConf.setNumReduceTasks(NUM_PARTITIONS);
    jobConf.set(JobContext.TASK_ATTEMPT_ID, "test_attempt");
    
    // Fake out a bunch of stuff to make a task context.
    MapOutputFile output = new YarnOutputFiles();
    output.setConf(jobConf);

    Progress mapProgress = new Progress();
    mapProgress.addPhase("map");
    mapProgress.addPhase("sort");

    MapTask mapTask = Mockito.mock(MapTask.class);
    Mockito.doReturn(output).when(mapTask).getMapOutputFile();
    Mockito.doReturn(true).when(mapTask).isMapTask();
    Mockito.doReturn(new TaskAttemptID("fake-jt", 12345, TaskType.MAP, 1, 1)).when(mapTask).getTaskID();
    Mockito.doReturn(mapProgress).when(mapTask).getSortPhase();
    
    MapTask t = new MapTask();
    Constructor<TaskReporter> constructor =
        TaskReporter.class.getDeclaredConstructor(Task.class,
            Progress.class, TaskUmbilicalProtocol.class);
    constructor.setAccessible(true);
    TaskReporter reporter = constructor.newInstance(t, mapProgress, null);
    reporter.setProgress(0.0f);
    Context context = new MapOutputCollector.Context(mapTask, jobConf, reporter);

    // Actually run the map sort.
    ResourceTimer timer = new ResourceTimer();
    MapOutputCollector<?,?> collector = ReflectionUtils.newInstance(collectorClazz, jobConf);
    collector.init(context);
    collectRecords(collector);
    collector.flush();
    collector.close();

    // Print results
    System.out.println("---------------------");
    System.out.println("Results for " + collectorClazz.getName() + ":");
    System.out.println("CPU time: " + timer.elapsedCpu() + "ms");
    System.out.println("CPU time (only this thread): " + timer.elapsedCpuThisThread() + "ms");
    System.out.println("Compilation time: " + timer.elapsedCompilation());
    System.out.println("GC time: " + timer.elapsedGC());
    System.out.println("Wall time: " + timer.elapsedWall());
    System.out.println("---------------------");
  }

  /**
   * Collect a bunch of random terasort-like records into the map output
   * collector.
   */
  @SuppressWarnings("unchecked")
  private void collectRecords(MapOutputCollector collector)
      throws IOException, InterruptedException {

    // The 10-90 key-value split is the same as terasort.
    BytesWritable key = new BytesWritable();
    key.setSize(10);
    BytesWritable val = new BytesWritable();
    val.setSize(90);
    byte[] keyBytes = key.getBytes();

    // Use a consistent random seed so that different implementations are
    // sorting the exact same data.
    XorshiftRandom r = new XorshiftRandom(1);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int partition = i % NUM_PARTITIONS;
      r.nextBytes(keyBytes);
      collector.collect(key, val, partition);
    }
  }
  
  public void runTest(Class<? extends MapOutputCollector> collector) throws Exception {
    for (int i = 0; i < 3; i++) {
      // GC a few times first so we're really just testing the collection, nothing else.
      System.gc();
      System.gc();
      System.gc();
      doBenchmark(collector);
    }
  }

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 1,
        "Usage: " + MapOutputCollectorBenchmark.class.getName() +
        " <collector class name>");
    Class<?> clazz = Class.forName(args[0]);
    Class<? extends MapOutputCollector> collectorClazz =
        clazz.asSubclass(MapOutputCollector.class);
    new MapOutputCollectorBenchmark().runTest(collectorClazz);
  }
}
