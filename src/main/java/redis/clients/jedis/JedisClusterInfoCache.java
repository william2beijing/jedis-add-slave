package redis.clients.jedis;

import redis.clients.util.ClusterNodeInformation;
import redis.clients.util.ClusterNodeInformationParser;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JedisClusterInfoCache {
  public static final ClusterNodeInformationParser nodeInfoParser = new ClusterNodeInformationParser();

  private Map<String, JedisPool> nodes = new HashMap<String, JedisPool>();
  private Map<Integer, List<JedisPool>> slots = new HashMap<Integer, List<JedisPool>>();

  private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
  private final Lock r = rwl.readLock();
  private final Lock w = rwl.writeLock();
  private final GenericObjectPoolConfig poolConfig;

  private int connectionTimeout;
  private int soTimeout;

  public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig, int timeout) {
    this(poolConfig, timeout, timeout);
  }

  public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig,
      final int connectionTimeout, final int soTimeout) {
    this.poolConfig = poolConfig;
    this.connectionTimeout = connectionTimeout;
    this.soTimeout = soTimeout;
  }

  public void discoverClusterNodesAndSlots(Jedis jedis) {
    w.lock();

    try {
      this.nodes.clear();
      this.slots.clear();

      final Map<String, List<JedisPool>> slots = new HashMap<String, List<JedisPool>>();

      final String nodeInfos = jedis.clusterNodes();
      // master
      for( String nodeInfo : nodeInfos.split("\n") ) {
        if( -1 != nodeInfo.indexOf("master") ) {
          ClusterNodeInformation clusterNodeInfo = nodeInfoParser.parse(nodeInfo, new HostAndPort(
              jedis.getClient().getHost(), jedis.getClient().getPort()));
          HostAndPort node = clusterNodeInfo.getNode();
          setNodeIfNotExist(node,false);
          assignSlotsToNode(slots,clusterNodeInfo.getAvailableSlots(),node);
    	}
      }
      // slave
      for( String nodeInfo : nodeInfos.split("\n") ) {
        if( -1 != nodeInfo.indexOf("slave") ) {
          ClusterNodeInformation clusterNodeInfo = nodeInfoParser.parse(nodeInfo, new HostAndPort(
              jedis.getClient().getHost(), jedis.getClient().getPort()));
          HostAndPort node = clusterNodeInfo.getNode();
          setNodeIfNotExist(node,true);
          assignSlotsToNode(slots,nodeInfo,node);
        }
      }
    } finally {
      w.unlock();
    }
  }

  public void discoverClusterSlots(Jedis jedis) {
    w.lock();

    try {
      this.slots.clear();

      final List<Object> slots = jedis.clusterSlots();

      for( Object slotInfoObj : slots ) {
        final List<Object> slotInfo = (List<Object>)slotInfoObj;

        if( 2 >= slotInfo.size() ) {
          continue;
        }

        final List<Integer> slotNums = getAssignedSlotArray(slotInfo);

        final List<JedisPool> pools = new ArrayList<JedisPool>();

        // master
        List<Object> hostInfos = (List<Object>)slotInfo.get(2);
        if( 0 >= hostInfos.size() ) {
          continue;
        }
        HostAndPort node = generateHostAndPort(hostInfos);

        setNodeIfNotExist(node,false);
        assignSlotsToNode(pools,slotNums,node);

        // slave
        hostInfos = (List<Object>)slotInfo.get(3);
        if( 0 >= hostInfos.size() ) {
          continue;
        }
        node = generateHostAndPort(hostInfos);

        setNodeIfNotExist(node,true);
        assignSlotsToNode(pools,node);
      }
    } finally {
      w.unlock();
    }
  }

  private HostAndPort generateHostAndPort(List<Object> hostInfos) {
    return new HostAndPort(SafeEncoder.encode((byte[])hostInfos.get(3)),SafeEncoder.encode((byte[])hostInfos.get(0)),
        ((Long)hostInfos.get(1)).intValue());
  }

  public void setNodeIfNotExist(HostAndPort node) {
    w.lock();
    try {
      String key = getNodeKey(node);
      if( nodes.containsKey(key) )
    	  return;
      JedisPool pool = new JedisPool(poolConfig,node.getHost(),node.getPort(),
          connectionTimeout,soTimeout,null,0,null,false);
      nodes.put(key,pool);
    } finally {
      w.unlock();
    }
  }

  public void setNodeIfNotExist(HostAndPort node, boolean isReadOnly) {
    String key = getNodeKey(node);
    if( nodes.containsKey(key) )
      return;
    JedisPool pool = new JedisPool(poolConfig,node.getHost(),node.getPort(),
        connectionTimeout,soTimeout,null,0,null,isReadOnly);
    nodes.put(key,pool);
  }

  public void assignSlotsToNode(Map<String, List<JedisPool>> slots, List<Integer> slotNums, HostAndPort node) {
    JedisPool pool = nodes.get(getNodeKey(node));
    if( null == pool ) {
      setNodeIfNotExist(node,false);
      pool = nodes.get(getNodeKey(node));
    }
    final List<JedisPool> pools = new ArrayList<JedisPool>();
    pools.add(pool);

    slots.put(node.getIdentity(),pools);

    for( Integer slot : slotNums ) {
      this.slots.put(slot,pools);
    }
  }

  public void assignSlotsToNode(Map<String, List<JedisPool>> slots, String nodeInfo, HostAndPort node) {
    JedisPool pool = nodes.get(getNodeKey(node));
    if( null == pool ) {
      setNodeIfNotExist(node,true);
      pool = nodes.get(getNodeKey(node));
    }
    final String[] nodeInfoArray = nodeInfo.split(" ");
    if( ClusterNodeInformationParser.SLOT_INFORMATIONS_MASTER_IDENTITY <= nodeInfoArray.length ) {
      final String masterIdentity = nodeInfoArray[ClusterNodeInformationParser.SLOT_INFORMATIONS_MASTER_IDENTITY];
      final List<JedisPool> pools = slots.get(masterIdentity);
      if( null != pools ) {
        pools.add(pool);
      }
    }
  }

  public void assignSlotsToNode(List<JedisPool> pools, List<Integer> slotNums, HostAndPort node) {
    JedisPool pool = nodes.get(getNodeKey(node));
    if( null == pool ) {
      setNodeIfNotExist(node,false);
      pool = nodes.get(getNodeKey(node));
    }
    pools.add(pool);

    for( Integer slot : slotNums ) {
      slots.put(slot,pools);
    }
  }

  public void assignSlotsToNode(List<JedisPool> pools, HostAndPort node) {
    JedisPool pool = nodes.get(getNodeKey(node));
    if( null == pool ) {
      setNodeIfNotExist(node,true);
      pool = nodes.get(getNodeKey(node));
    }
    pools.add(pool);
  }

  public JedisPool getSlotPool(int slot, boolean isWriteCommand) {
    r.lock();
    try {
      final List<JedisPool> pools = slots.get(slot);
      if( null == pools || pools.isEmpty() ) {
        return null;
      }
      if( isWriteCommand ) {
        return pools.get(0);
      }
      final Integer random = new Random().nextInt();
      final Integer index = Math.abs(random) % pools.size();
      return pools.get(index);
    } finally {
      r.unlock();
    }
  }

  public JedisPool getNode(String nodeKey) {
    r.lock();
    try {
      return nodes.get(nodeKey);
    } finally {
      r.unlock();
    }
  }

  public Map<String, JedisPool> getNodes() {
    r.lock();
    try {
      return new HashMap<String, JedisPool>(nodes);
    } finally {
      r.unlock();
    }
  }

  public static String getNodeKey(HostAndPort hnp) {
    return hnp.getHost() + ":" + hnp.getPort();
  }

  public static String getNodeKey(Client client) {
    return client.getHost() + ":" + client.getPort();
  }

  public static String getNodeKey(Jedis jedis) {
    return getNodeKey(jedis.getClient());
  }

  private List<Integer> getAssignedSlotArray(List<Object> slotInfo) {
    final List<Integer> slotNums = new ArrayList<Integer>();
    for( int slot = ((Long)slotInfo.get(0)).intValue(); slot <= ((Long)slotInfo.get(1)).intValue(); ++slot ) {
      slotNums.add(slot);
    }
    return slotNums;
  }
}
