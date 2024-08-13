package net.spy.memcached.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

public class PipedCollectionFuture<K, V>
        extends CollectionFuture<Map<K, V>> {
  private final ConcurrentLinkedDeque<Operation> ops = new ConcurrentLinkedDeque<>();
  private final AtomicReference<CollectionOperationStatus> operationStatus
          = new AtomicReference<>(null);

  private final Map<K, V> failedResult =
          new ConcurrentHashMap<>();

  public PipedCollectionFuture(CountDownLatch l, long opTimeout) {
    super(l, opTimeout);
  }

  @Override
  public boolean cancel(boolean ign) {
    Operation lastOp = ops.getLast();
    if (lastOp != null) {
      return lastOp.cancel("by application.");
    }
    return false;
  }

  @Override
  public boolean isCancelled() {
    Operation lastOp = ops.getLast();
    if (lastOp != null) {
      return lastOp.isCancelled();
    }
    return false;
  }

  public boolean hasErrored() {
    Operation lastOp = ops.getLast();
    return lastOp != null && lastOp.hasErrored();
  }

  @Override
  public boolean isDone() {
    return latch.getCount() == 0;
  }

  @Override
  public Map<K, V> get(long duration, TimeUnit unit)
          throws InterruptedException, TimeoutException, ExecutionException {

    long beforeAwait = System.currentTimeMillis();
    Operation lastOp = ops.getLast();
    if (!latch.await(duration, unit)) {
      if (lastOp.getState() != OperationState.COMPLETE) {
        MemcachedConnection.opTimedOut(lastOp);

        long elapsed = System.currentTimeMillis() - beforeAwait;
        throw new CheckedOperationTimeoutException(duration, unit, elapsed, lastOp);
      } else {
        for (Operation op : ops) {
          MemcachedConnection.opSucceeded(op);
        }
      }
    } else {
      // continuous timeout counter will be reset only once in pipe
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
