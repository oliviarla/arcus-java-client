/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached.bulkoperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.collection.BaseIntegrationTest;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class LopPipedInsertBulkMultipleValueTest extends BaseIntegrationTest {

  private String key = "LopPipedInsertBulkMultipleValueTest";

  @AfterEach
  @Override
  protected void tearDown() throws Exception {
    mc.delete(key).get();
    super.tearDown();
  }

  @Test
  void testInsertAndGet() {
    String value = "MyValue";

    int valueCount = 510;
    List<Object> valueList = new ArrayList<>(valueCount);
    for (int i = 0; i < valueCount; i++) {
      valueList.add("MyValue" + i);
    }

    try {
      // REMOVE
      mc.asyncLopDelete(key, 0, 4000, true).get();

      // SET
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncLopPipedInsertBulk(key, 0, valueList, new CollectionAttributes());
      try {
        Map<Integer, CollectionOperationStatus> errorList = future.get(
                20000L, TimeUnit.MILLISECONDS);
        assertTrue(errorList.isEmpty());
      } catch (TimeoutException e) {
        future.cancel(true);
        e.printStackTrace();
        fail(e.getMessage());
      }

      // GET
      int errorCount = 0;
      List<Object> resultList = null;
      Future<List<Object>> f = mc.asyncLopGet(key, 0, valueCount, false,
              false);
      try {
        resultList = f.get();
      } catch (Exception e) {
        f.cancel(true);
        e.printStackTrace();
        fail(e.getMessage());
      }

      assertNotNull(resultList);
      assertFalse(resultList.isEmpty(), "Cached list is empty.");
      assertEquals(valueCount, resultList.size());

      for (int i = 0; i < resultList.size(); i++) {
        if (!resultList.get(i).equals(valueList.get(i))) {
          errorCount++;
        }
      }
      assertEquals(valueCount, resultList.size());
      assertEquals(0, errorCount);

      // REMOVE
      mc.asyncLopDelete(key, 0, 4000, true).get();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  void testErrorCount() {
    int valueCount = 1200;
    Object[] valueList = new Object[valueCount];
    Arrays.fill(valueList, "MyValue");

    try {
      // SET
      Future<Map<Integer, CollectionOperationStatus>> future = mc
              .asyncLopPipedInsertBulk(key, 0, Arrays.asList(valueList),
                      null);

      Map<Integer, CollectionOperationStatus> map = future.get(1000L,
              TimeUnit.MILLISECONDS);
      assertEquals(ArcusClient.MAX_PIPED_ITEM_COUNT + 1, map.size());
      assertEquals(map.get(ArcusClient.MAX_PIPED_ITEM_COUNT - 1).getResponse(),
              CollectionResponse.NOT_FOUND);
      assertEquals(map.get(ArcusClient.MAX_PIPED_ITEM_COUNT).getResponse(),
              CollectionResponse.CANCELED);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
