package net.spy.memcached.v2;

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.internal.CompositeException;

public class AbstractArcusResult<T> implements ArcusResult<T> {

  protected T value;

  private final List<Exception> exceptions = new ArrayList<>();

  public AbstractArcusResult(T value) {
    this.value = value;
  }

  @Override
  public T get() {
    return value;
  }

  @Override
  public boolean hasError() {
    return !exceptions.isEmpty();
  }

  @Override
  public CompositeException getErrors() {
    return new CompositeException(exceptions);
  }
}
