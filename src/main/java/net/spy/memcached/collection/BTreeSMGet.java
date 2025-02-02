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
package net.spy.memcached.collection;

import net.spy.memcached.MemcachedNode;

import java.util.List;

public interface BTreeSMGet<T> {

  public int headerCount = 4;

  public void setKeySeparator(String keySeparator);

  public String getSpaceSeparatedKeys();

  public MemcachedNode getMemcachedNode();

  public List<String> getKeyList();

  public String stringify();

  public String getCommand();

  public boolean headerReady(int spaceCount);

  public String getKey();

  public int getFlags();

  public Object getSubkey();

  public int getDataLength();

  public byte[] getEflag();

  public void decodeItemHeader(String itemHeader);

  public BTreeSMGet<T> clone(MemcachedNode node, List<String> keyList);
}
