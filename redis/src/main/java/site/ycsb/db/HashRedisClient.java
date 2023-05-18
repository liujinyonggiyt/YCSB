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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import site.ycsb.ByteIterator;
import site.ycsb.DBRow;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import javax.annotation.Nullable;
import java.util.*;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 * 以Hash形式存储
 * See {@code redis/README.md} for details.
 */
public class HashRedisClient extends AbstractRedisClient {

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return doRead(table, key, fields, result);
  }

  private Status doRead(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray = (String[]) fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(), new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return jedis.del(key) == 0 ? Status.ERROR : Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return doUpdate(table, key, values);
  }

  private Status doUpdate(String table, String key, Map<String, ByteIterator> values) {
    return jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status batchUpdate(String table, DBRow... rows) {
    Status result = Status.OK;
    if (jedis instanceof Jedis) {
      Jedis singleJedis = (Jedis) jedis;
      Pipeline pipeline = singleJedis.pipelined();
      for (DBRow dbRow : rows) {
        pipeline.hmset(dbRow.getKey(), StringByteIterator.getStringMap(dbRow.getFieldMap()));
      }
      pipeline.sync();
    } else {
      for (DBRow dbRow : rows) {
        Status status = doUpdate(table, dbRow.getKey(), dbRow.getFieldMap());
        if (!status.isOk()) {
          result = status;
        }
      }
    }
    return result;
  }

  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields, Vector<Map<String, ByteIterator>> result) {
    if (jedis instanceof Jedis) {
      Jedis singleJedis = (Jedis) jedis;
      Pipeline pipeline = singleJedis.pipelined();
      List<Response<?>> responseList = new ArrayList<>(keys.size());
      for (int index = 0; index < keys.size(); index++) {
        final String key = keys.get(index);
        @Nullable final Set<String> fieldSet = fields.get(index);
        if (null == fieldSet) {
          //查询所有field
          responseList.add(pipeline.hgetAll(key));
        } else {
          String[] fieldArray = fieldSet.toArray(new String[fieldSet.size()]);
          responseList.add(pipeline.hmget(key, fieldArray));
        }
      }

      pipeline.sync();

      for (int index = 0; index < keys.size(); index++) {
        Map<String, ByteIterator> fieldResultMap = new HashMap<>();
        @Nullable final Set<String> fieldSet = fields.get(index);
        if (null == fieldSet) {
          //查询所有field
          Response<Map<String, String>> response = (Response<Map<String, String>>) responseList.get(index);
          StringByteIterator.putAllAsByteIterators(fieldResultMap, response.get());
        }else{
          Response<List<String>> response = (Response<List<String>>) responseList.get(index);
          List<String> values = response.get();
          Iterator<String> fieldIterator = fieldSet.iterator();
          Iterator<String> valueIterator = values.iterator();

          while (fieldIterator.hasNext() && valueIterator.hasNext()) {
            fieldResultMap.put(fieldIterator.next(), new StringByteIterator(valueIterator.next()));
          }
          assert !fieldIterator.hasNext() && !valueIterator.hasNext();
        }
        result.add(fieldResultMap);
      }
    } else {
      for (int index = 0; index < keys.size(); index++) {
        Map<String, ByteIterator> fieldMap = new HashMap<>();

        @Nullable Set<String> fieldSet = fields.get(index);
        doRead(table, keys.get(index), fieldSet, fieldMap);

        result.add(fieldMap);
      }
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

}
