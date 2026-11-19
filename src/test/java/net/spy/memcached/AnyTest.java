package net.spy.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnyTest {

    private static final String KEY = "AnyTest:btree";
    private static final int ELEMENT_COUNT = 10000;
    private static final int BATCH_SIZE = 500;
    private static final int WARMUP_ITERATIONS = 5;
    private static final int MEASURED_ITERATIONS = 30;

    private static final byte[] VALUE = new byte[600];
    private static final CollectionAttributes ATTRS = new CollectionAttributes(
            0, (long) ELEMENT_COUNT, CollectionOverflowAction.error);

    // shared by every test so no test pays for connecting, and shut down once at the end
    private static ArcusClientPool mc;

    @BeforeClass
    public static void setUpClass() {
        new Random().nextBytes(VALUE);
        mc = ArcusClient.createArcusClientPool("localhost:2181", "test",
                new ConnectionFactoryBuilder(), 1);
    }

    @AfterClass
    public static void tearDownClass() {
        if (mc != null) {
            mc.shutdown();
        }
    }

    /** One run of a scenario. Only run() is timed; verify() is not. */
    private interface Scenario {
        void run() throws Exception;

        void verify() throws Exception;
    }

    @Test
    public void testBopPipedInsertBulk500x20ElementsLatency() throws Exception {
        final List<Map<Long, Object>> batches = new ArrayList<Map<Long, Object>>();
        for (int b = 0; b < ELEMENT_COUNT / BATCH_SIZE; b++) {
            Map<Long, Object> elements = new TreeMap<Long, Object>();
            for (long i = (long) b * BATCH_SIZE; i < (long) (b + 1) * BATCH_SIZE; i++) {
                elements.put(i, VALUE);
            }
            batches.add(elements);
        }

        benchmark("bop piped insert bulk (500 x 20 requests)", new Scenario() {
            private final List<CollectionFuture<Map<Integer, CollectionOperationStatus>>> futures =
                    new ArrayList<CollectionFuture<Map<Integer, CollectionOperationStatus>>>();

            public void run() throws Exception {
                futures.clear();
                for (Map<Long, Object> batch : batches) {
                    futures.add(mc.asyncBopPipedInsertBulk(KEY, batch, ATTRS));
                }
                for (CollectionFuture<Map<Integer, CollectionOperationStatus>> future : futures) {
                    future.get(10000, TimeUnit.MILLISECONDS);
                }
            }

            public void verify() throws Exception {
                for (CollectionFuture<Map<Integer, CollectionOperationStatus>> future : futures) {
                    Map<Integer, CollectionOperationStatus> failed = future.get();
                    assertTrue("failed elements: " + failed, failed.isEmpty());
                }
            }
        });
    }

    @Test
    public void testBopPipedInsertBulk10000ElementsLatency() throws Exception {
        final Map<Long, Object> elements = new TreeMap<Long, Object>();
        for (long i = 0; i < ELEMENT_COUNT; i++) {
            elements.put(i, VALUE);
        }

        benchmark("bop piped insert bulk (1 x 10000 request)", new Scenario() {
            private Map<Integer, CollectionOperationStatus> failed;

            public void run() throws Exception {
                // the client splits this into pipes of 500 elements internally
                failed = mc.asyncBopPipedInsertBulk(KEY, elements, ATTRS)
                        .get(10000, TimeUnit.MILLISECONDS);
            }

            public void verify() {
                assertTrue("failed elements: " + failed, failed.isEmpty());
            }
        });
    }

    /**
     * Runs the scenario WARMUP_ITERATIONS times without recording, then
     * MEASURED_ITERATIONS times recording the elapsed time of each run, and
     * prints a summary. The key is deleted before every run so each run
     * inserts into a fresh b+tree.
     */
    private void benchmark(String name, Scenario scenario) throws Exception {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            runOnce(scenario);
        }

        long[] samples = new long[MEASURED_ITERATIONS];
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            samples[i] = runOnce(scenario);
        }

        printResult(name, samples);
    }

    private long runOnce(Scenario scenario) throws Exception {
        mc.delete(KEY).get();
        // the server frees a deleted collection in the background; without this pause the
        // previous run's memory is not reclaimed yet and the key gets evicted mid-insert
        Thread.sleep(200);

        long start = System.nanoTime();
        scenario.run();
        long elapsed = System.nanoTime() - start;

        scenario.verify();
        CollectionAttributes stored = mc.asyncGetAttr(KEY).get(1000, TimeUnit.MILLISECONDS);
        assertEquals(Long.valueOf(ELEMENT_COUNT), stored.getCount());
        return elapsed;
    }

    private void printResult(String name, long[] samples) {
        long[] sorted = samples.clone();
        Arrays.sort(sorted);

        double sum = 0;
        for (long s : sorted) {
            sum += s;
        }
        double avg = sum / sorted.length;

        double sqDiff = 0;
        for (long s : sorted) {
            sqDiff += (s - avg) * (s - avg);
        }
        double stdDev = Math.sqrt(sqDiff / sorted.length);

        System.out.println("==================================================");
        System.out.println(name);
        System.out.printf("elements   : %d, value size: %d bytes%n", ELEMENT_COUNT, VALUE.length);
        System.out.printf("iterations : %d (after %d warmup)%n", sorted.length, WARMUP_ITERATIONS);
        System.out.println("--------------------------------------------------");
        System.out.printf("avg        : %.3f ms%n", avg / 1e6);
        System.out.printf("stddev     : %.3f ms%n", stdDev / 1e6);
        System.out.printf("min        : %.3f ms%n", sorted[0] / 1e6);
        System.out.printf("p50        : %.3f ms%n", percentile(sorted, 50) / 1e6);
        System.out.printf("p90        : %.3f ms%n", percentile(sorted, 90) / 1e6);
        System.out.printf("max        : %.3f ms%n", sorted[sorted.length - 1] / 1e6);
        System.out.printf("tps (avg)  : %.1f elements/s%n", ELEMENT_COUNT / (avg / 1e9));
        System.out.println("==================================================");
    }

    private long percentile(long[] sorted, double p) {
        int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
