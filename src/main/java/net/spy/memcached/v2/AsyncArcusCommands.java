package net.spy.memcached.v2;


import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.CachedData;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.Transcoder;

public class AsyncArcusCommands<T> {

  private final ArcusClient arcusClient;

  private final Transcoder<T> tc;

  private final OperationFactory opFact;

  public AsyncArcusCommands(ArcusClient arcusClient) {
    this.arcusClient = arcusClient;
    this.tc = new GenericTranscoder<>();
    this.opFact = arcusClient.getOpFactory();
  }

  public ArcusFuture<Boolean> set(String key, int exp, T value) {
    AtomicBoolean atomicBoolean = new AtomicBoolean();
    ArcusFuture<Boolean> arcusFuture = new ArcusFutureImpl<>(new SetResult(atomicBoolean));
    CachedData co = tc.encode(value);
    Operation op = opFact.store(StoreType.set, key, co.getFlags(), exp, co.getData(),
            status -> atomicBoolean.set(status.isSuccess()), arcusFuture::complete);
    arcusClient.addOp(key, op);
    arcusFuture.addOperations(Collections.singletonList(op));
    return arcusFuture;
  }
}
