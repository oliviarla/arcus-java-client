package net.spy.memcached.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

public class PipedCollectionFuture<K, V>
        extends CollectionFuture<Map<K, V>> {
  // operations that are completed or in progress
  private final List<Operation> ops = new ArrayList<>();
  private final AtomicReference<CollectionOperationStatus> operationStatus
          = new AtomicReference<>(null);

  private final Map<K, V> failedResult =
          new ConcurrentHashMap<>();

  public PipedCollectionFuture(CountDownLatch l, long opTimeout) {
    super(l, opTimeout);
  }

  @Override
  public boolean cancel(boolean ign) {
    return ops.get(ops.size() - 1).cancel("by application.");
  }

  /**
   * if previous op is cancelled, then next ops are not added to the opQueue.
   * So we only need to check current op.
   *
   * @return true if operation is cancelled.
   */
  @Override
  public boolean isCancelled() {
    return operationStatus.get().getResponse() == CollectionResponse.CANCELED;
  }

  /**
   * if previous op threw exception, then next ops are not added to the opQueue.
   * So we only need to check current op.
   *
   * @return true if operation has errored by exception.
   */
  public boolean hasErrored() {
    return ops.get(ops.size() - 1).hasErrored();
  }

  @Override
  public boolean isDone() {
    return latch.getCount() == 0;
  }

  @Override
  public Map<K, V> get(long duration, TimeUnit unit)
          throws InterruptedException, TimeoutException, ExecutionException {
    long beforeAwait = System.currentTimeMillis();
    Operation lastOp;
    if (!latch.await(duration, unit)) {
      lastOp = ops.get(ops.size() - 1);
      if (lastOp.getState() != OperationState.COMPLETE) {
        MemcachedConnection.opTimedOut(lastOp);

        long elapsed = System.currentTimeMillis() - beforeAwait;
        throw new CheckedOperationTimeoutException(duration, unit, elapsed, lastOp);
      }
    } else {
      // continuous timeout counter will be reset only once in pipe
      lastOp = ops.get(ops.size() - 1);
      MemcachedConnection.opSucceeded(lastOp);
    }

    if (lastOp != null && lastOp.hasErrored()) {
      throw new ExecutionException(lastOp.getException());
    }

    if (lastOp != null && lastOp.isCancelled()) {
      throw new ExecutionException(new RuntimeException(lastOp.getCancelCause()));
    }

    return failedResult;
  }

  @Override
  public CollectionOperationStatus getOperationStatus() {
    return operationStatus.get();
  }

  public void setOperationStatus(CollectionOperationStatus status) {
    if (operationStatus.get() == null) {
      operationStatus.set(status);
      return;
    }

    if (!status.isSuccess() && operationStatus.get().isSuccess()) {
      operationStatus.set(status);
    }
  }

  public void addEachResult(K index, V status) {
    failedResult.put(index, status);
  }

  public void addOperation(Operation op) {
    ops.add(op);
  }
}
