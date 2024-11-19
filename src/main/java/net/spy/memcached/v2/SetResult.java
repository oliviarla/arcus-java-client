package net.spy.memcached.v2;

import java.util.concurrent.atomic.AtomicBoolean;

public class SetResult extends AbstractArcusResult<Boolean> {

  private final AtomicBoolean atomicBoolean;

  public SetResult(AtomicBoolean atomicBoolean) {
    super(atomicBoolean.get());
    this.atomicBoolean = atomicBoolean;
  }

  @Override
  public Boolean get() {
    return value = atomicBoolean.get();
  }
}
