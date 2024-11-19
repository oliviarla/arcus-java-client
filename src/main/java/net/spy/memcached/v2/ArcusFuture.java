package net.spy.memcached.v2;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.spy.memcached.ops.Operation;

public interface ArcusFuture<T> extends CompletionStage<T>, Future<T> {

  void complete();

  void addOperations(List<Operation> ops);

  boolean hasError();

  ExecutionException getError();

}
