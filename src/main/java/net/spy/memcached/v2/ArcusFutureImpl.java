package net.spy.memcached.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import net.spy.memcached.internal.CompositeException;
import net.spy.memcached.ops.Operation;

public class ArcusFutureImpl<T> extends CompletableFuture<T> implements ArcusFuture<T> {

  private final List<Operation> ops = new ArrayList<>();

  private final ArcusResult<T> result;

  public ArcusFutureImpl(ArcusResult<T> result) {
    this.result = result;
  }

  @Override
  public void complete() {
    if (this.hasError()) {
      this.completeExceptionally(this.getError());
    } else {
      this.complete(this.result.get());
    }
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (!this.isDone()) {
      boolean cancelled = ops.stream()
              .map((op) -> op.cancel("by application"))
              .reduce(false, (sum, current) -> sum || current);
      return cancelled && super.cancel(mayInterruptIfRunning);
    }
    return false;
  }

  @Override
  public void addOperations(List<Operation> ops) {
    this.ops.addAll(ops);
  }

  @Override
  public boolean hasError() {
    return this.ops.stream().anyMatch(Operation::hasErrored);
  }

  @Override
  public ExecutionException getError() {
    List<Exception> exceptions = this.ops.stream()
            .map(Operation::getException)
            .collect(Collectors.toList());
    return new CompositeException(exceptions);
  }
}
