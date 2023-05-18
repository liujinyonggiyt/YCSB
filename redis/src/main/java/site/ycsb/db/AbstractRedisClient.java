package site.ycsb.db;

import redis.clients.jedis.*;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.workloads.CoreWorkload;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import static site.ycsb.workloads.CoreWorkload.*;
import static site.ycsb.workloads.CoreWorkload.ZERO_PADDING_PROPERTY_DEFAULT;

/**
 * @author liujinyong
 * @create 2023/5/11 2:50 PM
 */
public abstract class AbstractRedisClient extends DB {
  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  protected JedisCommands jedis;
  private String keyPrefix;
  private int zeropadding;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    //scan参数校验
    {
      double scanproportion = Double.parseDouble(props.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
      if (0 != Double.compare(scanproportion, 0D) && props.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
        throw new RuntimeException(this.getClass().getSimpleName() + " not support unordered inserts scan!");
      }
    }

    keyPrefix = props.getProperty(CoreWorkload.KEY_NAME_PREFIX, CoreWorkload.KEY_NAME_PREFIX_DEFAULT);
    zeropadding = Integer.parseInt(props.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      jedis = new JedisCluster(jedisClusterNodes);
    } else {
      String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
      if (redisTimeout != null) {
        jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
      } else {
        jedis = new Jedis(host, port);
      }
      ((Jedis) jedis).connect();
    }

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedis).auth(password);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    long startLongKey = Long.parseLong(startkey.replaceFirst(keyPrefix, ""));
    String[] keyArray = new String[recordcount];
    for (int offset = 0; offset < recordcount; offset++) {
      final long scanKeyNum = startLongKey + offset;
      final String scanKey = CoreWorkload.buildKeyName(keyPrefix, scanKeyNum, zeropadding, true);
      keyArray[offset] = scanKey;
    }

    mget(keyArray);
    return Status.OK;
  }

  protected List<String> mget(String... keys) {
    if (jedis instanceof Jedis) {
      Jedis singleJedis = (Jedis) jedis;
      return singleJedis.mget(keys);
    } else if (jedis instanceof JedisCluster) {
      JedisCluster jedisCluster = (JedisCluster) jedis;
      return jedisCluster.mget(keys);
    } else {
      throw new UnsupportedOperationException(jedis.getClass().getSimpleName());
    }
  }

  protected String mset(String... keysvalues) {
    if (jedis instanceof Jedis) {
      Jedis singleJedis = (Jedis) jedis;
      return singleJedis.mset(keysvalues);
    } else if (jedis instanceof JedisCluster) {
      JedisCluster jedisCluster = (JedisCluster) jedis;
      return jedisCluster.mset(keysvalues);
    } else {
      throw new UnsupportedOperationException(jedis.getClass().getSimpleName());
    }
  }
}
