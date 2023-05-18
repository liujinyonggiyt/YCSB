/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.*;

import java.util.*;

import static site.ycsb.workloads.CoreWorkload.FIELD_COUNT_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 * K/V形式存储，只支持一个field，将field的值转换为value
 *
 * See {@code redis/README.md} for details.
 */
public class KVRedisClient extends AbstractRedisClient {
  @Override
  public void init() throws DBException {
    super.init();
    Properties props = getProperties();
    //field为1
    int fieldcount = Integer.parseInt(props.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    if (1 != fieldcount) {
      throw new UnsupportedOperationException("fieldcount must is 1!fieldcount:" + fieldcount);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    String value = jedis.get(key);
    if(null != fields){
      assert fields.size() == 1;
      result.put(fields.iterator().next(), new StringByteIterator(value));
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    assert values.size() == 1;
    if (jedis.set(key, values.values().iterator().next().toString()).equals("OK")) {
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return jedis.del(key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    assert values.size() == 1;
    return jedis.set(key, values.values().iterator().next().toString())
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status batchUpdate(String table, DBRow... rows) {
    String[] keysvalues = new String[rows.length * 2];
    for (int index = 0; index < rows.length; index++) {
      DBRow dbRow = rows[index];
      keysvalues[2*index] = dbRow.getKey();
      Map<String, ByteIterator> fieldMap = dbRow.getFieldMap();
      assert fieldMap.size() == 1;
      keysvalues[2*index + 1] = fieldMap.values().iterator().next().toString();
    }
    return mset(keysvalues).equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields, Vector<Map<String, ByteIterator>> result) {
    List<String> valueList = mget(keys.toArray(new String[0]));
    Status status = Status.OK;
    for (int index = 0; index < keys.size(); index++) {
      final String value = valueList.get(index);
      Map<String, ByteIterator> fieldMap = new HashMap<>();
      if(null == value){
        status = Status.ERROR;
      }else{
        Set<String> fieldSet = fields.get(index);
        if(null!=fieldSet){
          assert fieldSet.size() == 1;
          fieldMap.put(fieldSet.iterator().next(), new StringByteIterator(value));
        }
      }
      result.add(fieldMap);
    }
    return status;
  }
}
