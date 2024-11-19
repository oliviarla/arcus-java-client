package net.spy.memcached;

import net.spy.memcached.v2.ArcusFuture;
import net.spy.memcached.v2.AsyncArcusCommands;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AnyTest {

  @Test
  void set() {
    ArcusClient arcusClient = ArcusClient.createArcusClient("localhost:2181", "test");
    AsyncArcusCommands<Object> async = new AsyncArcusCommands<>(arcusClient);

    ArcusFuture<Boolean> future = async.set("key", 0, "value");
    future.cancel(true);
    future.whenComplete((result, throwable) -> {
      if (throwable != null) {
        fail();
      } else {
        assertTrue(result);
      }
    });
  }

}
