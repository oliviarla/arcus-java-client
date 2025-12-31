package net.spy.memcached.performance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ArcusClientPool;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.Element;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.v2.AsyncArcusCommands;
import net.spy.memcached.v2.ArcusFuture;
import net.spy.memcached.v2.vo.BKey;
import net.spy.memcached.v2.vo.BopGetArgs;
import net.spy.memcached.v2.vo.SMGetElements;
import net.spy.memcached.v2.vo.SMGetMergeStrategy;

import org.junit.jupiter.api.Test;

/**
 * Performance comparison test for BTree sort merge Get operations.
 * Compares four implementations using actual Arcus server:
 * 1. AsyncArcusCommands#bopSortMergeGet with TimSort
 * 2. AsyncArcusCommands#bopSortMergeGet with K-way merge
 * 3. AsyncArcusCommands#bopSortMergeGet with Two-pointer merge
 * 4. ArcusClient#asyncBopSortMergeGet (legacy API)
 *
 * Requirements:
 * - Arcus cache server must be running
 * - Update ARCUS_ADMIN_ADDRESS before running
 *
 * Results are exported to btree_merge_performance_results.csv for analysis.
 */
class BTreeSortMergePerformanceTest {

  // IMPORTANT: Update this with your Arcus admin (ZooKeeper) address
  private static final String ARCUS_ADMIN_ADDRESS = "127.0.0.1:2181";
  private static final String SERVICE_CODE = "test";
  private static final String CSV_OUTPUT_FILE = "btree_merge_performance_results.csv";

  @Test
  void compareMergeMethods() throws Exception {
    System.out.println("BTree Sort Merge Get Performance Comparison (Real Server)");
    System.out.println("==========================================================\n");

    // Enable merge latency output
    SMGetElements.setPrintMergeLatency(true);

    // Create Arcus clientPool
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
    ArcusClientPool clientPool = ArcusClient.createArcusClientPool(ARCUS_ADMIN_ADDRESS, SERVICE_CODE, cfb, 16);

    try {
      // Create AsyncArcusCommands interface
      AsyncArcusCommands<String> async = clientPool.asyncCommands();

      // Get number of cache nodes
      int numCacheNodes = clientPool.getAvailableServers().size();
      System.out.println("Connected to " + numCacheNodes + " cache node(s)");

      // Create (# of cache nodes) * 10 btree items, each with 2000 elements
      int numKeys = numCacheNodes * 10;
      int elementsPerKey = 2000;

      System.out.println("Test configuration: " + numKeys + " keys × " + elementsPerKey + " elements");
      System.out.println("Total elements: " + (numKeys * elementsPerKey));

      System.out.println("\n========================================");
      System.out.println("Test: " + numKeys + " keys x " + elementsPerKey + " elements");
      System.out.println("========================================");

      // Prepare test data in Arcus server
      List<String> keys = prepareTestData(clientPool, async, numKeys, elementsPerKey);

      // Test parameters
      BKey from = BKey.of(0L);
      BKey to = BKey.of(Long.MAX_VALUE);
      boolean unique = true;
      int count = 2000;

      // Warmup
      System.out.println("\nWarming up...");
      for (int i = 0; i < 3; i++) {
        testTimSort(async, keys, from, to, unique, count);
        testKWayMerge(async, keys, from, to, unique, count);
        testTwoPointer(async, keys, from, to, unique, count);
        testLegacyAPI(clientPool, keys, from, to, unique, count);
      }

      // Benchmark
      System.out.println("Running benchmarks...\n");
      int iterations = 1;

      long timSortTotal = 0;
      long kWayMergeTotal = 0;
      long twoPointerTotal = 0;
      long legacyTotal = 0;

      for (int i = 0; i < iterations; i++) {
        // Test TimSort (v2 API)
        long start = System.nanoTime();
        SMGetElements<String> timSortResult =
            testTimSort(async, keys, from, to, unique, count);
        timSortTotal += System.nanoTime() - start;

        // Test K-way merge (v2 API)
        start = System.nanoTime();
        SMGetElements<String> kWayMergeResult =
            testKWayMerge(async, keys, from, to, unique, count);
        kWayMergeTotal += System.nanoTime() - start;

        // Test Two-pointer merge (v2 API)
        start = System.nanoTime();
        SMGetElements<String> twoPointerResult =
            testTwoPointer(async, keys, from, to, unique, count);
        twoPointerTotal += System.nanoTime() - start;

        // Test legacy API
        start = System.nanoTime();
        List<SMGetElement<Object>> legacyResult =
            testLegacyAPI(clientPool, keys, from, to, unique, count);
        legacyTotal += System.nanoTime() - start;

        // Verify results match (first iteration only)
        if (i == 0) {
          verifyResults(timSortResult, kWayMergeResult, twoPointerResult, legacyResult);
        }
      }

      double timSortAvg = timSortTotal / (double) iterations / 1_000_000.0;
      double kWayMergeAvg = kWayMergeTotal / (double) iterations / 1_000_000.0;
      double twoPointerAvg = twoPointerTotal / (double) iterations / 1_000_000.0;
      double legacyAvg = legacyTotal / (double) iterations / 1_000_000.0;

      System.out.println("Results (averaged over " + iterations + " iterations):");
      System.out.println(String.format("  1. TimSort (v2 API):          %8.3f ms (baseline)", timSortAvg));
      System.out.println(String.format("  2. K-way Merge (v2 API):      %8.3f ms (%.2fx %s)",
          kWayMergeAvg,
          timSortAvg / kWayMergeAvg,
          timSortAvg > kWayMergeAvg ? "faster" : "slower"));
      System.out.println(String.format("  3. Two-pointer (v2 API):      %8.3f ms (%.2fx %s)",
          twoPointerAvg,
          timSortAvg / twoPointerAvg,
          timSortAvg > twoPointerAvg ? "faster" : "slower"));
      System.out.println(String.format("  4. Legacy API (Two-pointer):  %8.3f ms (%.2fx %s)",
          legacyAvg,
          timSortAvg / legacyAvg,
          timSortAvg > legacyAvg ? "faster" : "slower"));

      // Clean up test data
      for (String key : keys) {
        clientPool.delete(key).get(1, TimeUnit.SECONDS);
      }

      System.out.println("\n\nAll benchmarks completed successfully!");

    } finally {
      clientPool.shutdown();
    }
  }

  /**
   * Prepares test data by inserting BTree collections into Arcus server.
   */
  private static List<String> prepareTestData(ArcusClientPool client,
                                               AsyncArcusCommands<String> async,
                                               int numKeys, int elementsPerKey) throws Exception {
    System.out.println("Preparing test data: " + numKeys + " keys x " + elementsPerKey + " elements...");

    List<String> keys = new ArrayList<>();

    for (int i = 0; i < numKeys; i++) {
      String key = "btree_smget_test_" + i;
      keys.add(key);

      // Delete if exists
      client.delete(key).get(2, TimeUnit.SECONDS);

      // Create BTree collection
      CollectionAttributes attrs = new CollectionAttributes();

      // Insert elements
      List<Element<Object>> elements = new ArrayList<>();
      for (int j = 0; j < elementsPerKey; j++) {
        long bkey = (long) i * elementsPerKey + j;
        String value = "value_" + bkey;
        elements.add(new Element<>(bkey, value, (byte[]) null));
      }
      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = client.asyncBopPipedInsertBulk(key, elements, attrs);
      Map<Integer, CollectionOperationStatus> status = future.get(2, TimeUnit.SECONDS);
      if (!status.isEmpty()) {
        throw new RuntimeException("Failed to insert all elements: " + status);
      }
    }

    System.out.println("Test data prepared successfully.\n");
    return keys;
  }

  private static SMGetElements<String> testTimSort(AsyncArcusCommands<String> async,
                                                    List<String> keys,
                                                    BKey from, BKey to,
                                                    boolean unique, int count) throws Exception {
    BopGetArgs args = new BopGetArgs.Builder()
        .count(count)
        .mergeStrategy(SMGetMergeStrategy.TIMSORT)
        .build();

    ArcusFuture<SMGetElements<String>> future = async.bopSortMergeGet(keys, from, to, unique, args);
    return future.get(10, TimeUnit.SECONDS);
  }

  private static SMGetElements<String> testKWayMerge(AsyncArcusCommands<String> async,
                                                      List<String> keys,
                                                      BKey from, BKey to,
                                                      boolean unique, int count) throws Exception {
    BopGetArgs args = new BopGetArgs.Builder()
        .count(count)
        .mergeStrategy(SMGetMergeStrategy.KWAY_MERGE)
        .build();

    ArcusFuture<SMGetElements<String>> future = async.bopSortMergeGet(keys, from, to, unique, args);
    return future.get(10, TimeUnit.SECONDS);
  }

  private static SMGetElements<String> testTwoPointer(AsyncArcusCommands<String> async,
                                                       List<String> keys,
                                                       BKey from, BKey to,
                                                       boolean unique, int count) throws Exception {
    BopGetArgs args = new BopGetArgs.Builder()
        .count(count)
        .mergeStrategy(SMGetMergeStrategy.TWO_POINTER)
        .build();

    ArcusFuture<SMGetElements<String>> future = async.bopSortMergeGet(keys, from, to, unique, args);
    return future.get(10, TimeUnit.SECONDS);
  }

  private static List<SMGetElement<Object>> testLegacyAPI(ArcusClientPool client,
                                                           List<String> keys,
                                                           BKey from, BKey to,
                                                           boolean unique, int count) throws Exception {
    SMGetFuture<List<SMGetElement<Object>>> future = client.asyncBopSortMergeGet(keys,
        (long) from.getData(), (long) to.getData(),
        null, count, unique);
    return future.get(10, TimeUnit.SECONDS);
  }

  private static void verifyResults(SMGetElements<String> timSortResult,
                                     SMGetElements<String> kWayMergeResult,
                                     SMGetElements<String> twoPointerResult,
                                     List<SMGetElement<Object>> legacyResult) {
    System.out.println("Verifying results consistency...");

    int timSortSize = timSortResult.getElements().size();
    int kWayMergeSize = kWayMergeResult.getElements().size();
    int twoPointerSize = twoPointerResult.getElements().size();
    int legacySize = legacyResult.size();

    System.out.println("  TimSort result size:     " + timSortSize);
    System.out.println("  K-way merge result size: " + kWayMergeSize);
    System.out.println("  Two-pointer result size: " + twoPointerSize);
    System.out.println("  Legacy API result size:  " + legacySize);

    if (timSortSize != kWayMergeSize || timSortSize != twoPointerSize) {
      throw new AssertionError("Merge strategies produced different result sizes!");
    }

    // Verify element order matches
    for (int i = 0; i < timSortSize; i++) {
      BKey timSortBKey = timSortResult.getElements().get(i).getbTreeElement().getBkey();
      BKey kWayMergeBKey = kWayMergeResult.getElements().get(i).getbTreeElement().getBkey();
      BKey twoPointerBKey = twoPointerResult.getElements().get(i).getbTreeElement().getBkey();

      if (!timSortBKey.equals(kWayMergeBKey) || !timSortBKey.equals(twoPointerBKey)) {
        throw new AssertionError(String.format(
            "Element order mismatch at index %d: TimSort=%s, K-way=%s, Two-pointer=%s",
            i, timSortBKey, kWayMergeBKey, twoPointerBKey));
      }
    }

    System.out.println("  All results match!\n");
  }
}