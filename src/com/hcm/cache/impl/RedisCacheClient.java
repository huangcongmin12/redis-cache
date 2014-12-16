package com.hcm.cache.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.hcm.utils.SerializeUtil;

/**
 * redis的客户端实现
 *
 */
public class RedisCacheClient {
	private static final Logger log = Logger.getLogger(RedisCacheClient.class);
	private JedisPool pool;
	private JedisPoolConfig config;
	private static final String HOSTS_KEY = "hosts";
	private static final String TIMEOUT_KEY = "timeOut";
	private static final String MAXACTIVE_KEY = "maxActive";
	private static final String MAXIDLE_KEY = "maxIdle";
	private static final String MAXWAIT_KEY = "maxWait";
	private static final String TESTONBORROW_KEY = "testOnBorrow";
	private static final String TESTONRETURN_KEY = "testOnReturn";
	
	private String parameters;

	public RedisCacheClient(String parameter) {
		parameters = parameter;
		createPool();
	}
	private synchronized boolean createPool(){
		boolean result = false;
		if(!canConnection()){
			log.debug("-----------------------创建JedisPool------------------------begin---");

			try {
				JSONObject json = JSONObject.fromObject(parameters);
				config = new JedisPoolConfig();
				config.setMaxActive(json.getInt(MAXACTIVE_KEY));
				config.setMaxIdle(json.getInt(MAXIDLE_KEY));
				config.setMaxWait(json.getLong(MAXWAIT_KEY));
				config.setTestOnBorrow(json.getBoolean(TESTONBORROW_KEY));
				config.setTestOnReturn(json.getBoolean(TESTONRETURN_KEY));
				String hosts = json.getString(HOSTS_KEY);
				String[] hostArr = hosts.split(",");
				if(hostArr!=null&&hostArr.length>0){
					for(int i=0;i<hostArr.length;i++){
						String ip = hostArr[i].split(":")[0];
						int port = Integer.parseInt(hostArr[i].split(":")[1]);
						pool = new JedisPool(config, ip,
								port, json.getInt(TIMEOUT_KEY));
						if(canConnection()){
							log.info("-----------------JedisPool使用主机信息:"+ip+":"+port);
							result = true;
							break;
						}
					}
				}
			} catch (Exception e) {
				log.error("",e);
			}
			log.debug("-----------------------创建JedisPool------------------------end---");
		}
		return result;
	}

	/**
	 * redis是否可用
	 * @return
	 */
	private  boolean canConnection() {
		if(pool == null)
			return false;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.connect();
			jedis.get("ok");
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException){
				return false;
			}
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}	
		return true;
	}
	
	public void addItemToList(int dbIndex, String key, Object object) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.connect();
			jedis.select(dbIndex);
			jedis.lpush(key.getBytes(), SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public List<Object> getItemFromList(int dbIndex, String key) {
		Jedis jedis = null;
		List<byte[]> ss = null;
		List<Object> data = new ArrayList<Object>();
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			long len = jedis.llen(key);
			if (len == 0)
				return null;
			ss = jedis.lrange(key.getBytes(), 0, (int) len - 1);
			for (int i = 0; i < len; i++) {
				data.add(SerializeUtil.unserialize(ss.get(i)));
			}
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return data;

	}

	public void addItem(int dbIndex, String key, Object object) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			jedis.set(key.getBytes(), SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public String flushDB(int dbIndex) {
		Jedis jedis = null;
		String result = "";
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			result = jedis.flushDB();
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return result;
	}

	public void addItem(int dbIndex, String key, Object object, int seconds) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			jedis.setex(key.getBytes(), seconds,
					SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public Object getItem(int dbIndex, String key) {
		Jedis jedis = null;
		byte[] data = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			data = jedis.get(key.getBytes());
			return SerializeUtil.unserialize(data);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return null;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public long delItem(int dbIndex, String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			return jedis.del(key.getBytes());
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return 0;
	}

	public long getIncrement(int dbIndex, String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			return jedis.incr(key);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return 0L;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	
	/**
	 * 实现 incrBy 的方法；对 key 对应的值 增、减对应的量；
	 * @param dbIndex
	 * @param key
	 * @param increment
	 * @return
	 */
	public long getIncrementBy(int dbIndex, String key, long increment) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			return jedis.incrBy(key, increment);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return 0L;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	/**
	 * 
	 * 
	 * @param key
	 * @param map
	 */
	public void addMap(int dbIndex, String key, Map<String, String> map) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			if (map != null && !map.isEmpty()) {
				for (String mapkey:map.keySet()) {
					jedis.hset(key, mapkey, map.get(mapkey));
				}

			}
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public Map<String, String> getMap(int dbIndex, String key) {
		Map<String, String> map = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			map = jedis.hgetAll(key);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return map;

	}
	public String getMapItem(int dbIndex,String key,String field) {
		String value = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			value = jedis.hget(key, field);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return value;

	}
	
	/**
	 * 提供 reids 的 hincrBy 的方法；对map 下的 key\field 的值，进行增减；
	 * add by yugn 2014.10.1
	 * @param dbIndex
	 * @param key
	 * @param field
	 * @param increment
	 * @return
	 */
	public long getMapIncrementBy(int dbIndex, String key, String field, long increment) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			return jedis.hincrBy(key, field, increment);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return -1L;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	
	/**
	 * 根据 key , field 判断是否存在；
	 * @param dbIndex
	 * @param key
	 * @param field
	 * @return
	 */
	public boolean isMapFieldExists(int dbIndex, String key, String field){
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			return jedis.hexists(key, field);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return false;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	/**
	 * 
	 * 
	 * @param key
	 * @param set
	 */
	public void addSet(int dbIndex, String key, Set<String> set) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			if (set != null && !set.isEmpty()) {
				for (String value : set) {
					jedis.sadd(key, value);
				}
			}
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public Set<String> getSet(int dbIndex, String key) {
		Set<String> sets = new HashSet<String>();
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			sets = jedis.smembers(key);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

		return sets;
	}
	
	/**
	 * 
	 * 
	 * @param key
	 * @param list
	 */
	public void addList( int dbIndex,String key, List<String> list) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			if (list != null && !list.isEmpty()) {
				for (String value : list) {
					jedis.lpush(key, value);
				}
			}
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	
	public List<String> getList(int dbIndex, String key) {
		List<String> lists = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			lists = jedis.lrange(key, 0, -1);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

		return lists;
	}
	
	public void destroyPool() {
		if(null!=pool) {
			pool.destroy();
		}
	}
	
	
	public void addItem(String key, Object object) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.set(key.getBytes(), SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}


	public void addItem(String key, Object object, int seconds) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.setex(key.getBytes(), seconds,
					SerializeUtil.serialize(object));
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public Object getItem(String key) {
		Jedis jedis = null;
		byte[] data = null;
		try {
			jedis = pool.getResource();
			data = jedis.get(key.getBytes());
			return SerializeUtil.unserialize(data);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return null;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public long delItem(String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.del(key.getBytes());
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return 0;
		
	}
	
	public long getIncrement(String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.incr(key);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return 0L;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	
	/**
	 * 实现 incrBy 的方法；对 key 对应的值 增、减对应的量；
	 * @param key
	 * @param increment
	 * @return
	 */
	public long getIncrementBy(String key, long increment) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.incrBy(key, increment);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return 0L;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	
	public void addItemFile(String key, byte[] file) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.set(key.getBytes(), file);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	
	
	
	/**
	 * 
	 * 
	 * @param key
	 * @param map
	 */
	public void addMap(String key, Map<String, String> map) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (map != null && !map.isEmpty()) {
				for (String mapkey:map.keySet()) {
					jedis.hset(key, mapkey, map.get(mapkey));
				}
			}
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}

	public Map<String, String> getMap(String key) {
		Map<String, String> map = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			map = jedis.hgetAll(key);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return map;
	}
	
	public String getMapItem(String key,String field) {
		String value = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			value = jedis.hget(key, field);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return value;

	}
	
	/**
	 * 提供 reids 的 hincrBy 的方法；对map 下的 key\field 的值，进行增减；
	 * add by yugn 2014.10.1
	 * @param key
	 * @param field
	 * @param increment
	 * @return
	 */
	public long getMapIncrementBy(String key, String field, long increment) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.hincrBy(key, field, increment);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return -1L;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	
	/**
	 * 根据 key , field 判断是否存在；
	 * @param key
	 * @param field
	 * @return
	 */
	public boolean isMapFieldExists(String key, String field){
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.hexists(key, field);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return false;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	/**
	 * 
	 * 
	 * @param key
	 * @param set
	 */
	public void addSet( String key, Set<String> set) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (set != null && !set.isEmpty()) {
				for (String value : set) {
					jedis.sadd(key, value);
				}
			}
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	public Set<String> getSet(String key) {
		Set<String> sets = new HashSet<String>();
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			sets = jedis.smembers(key);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

		return sets;
	}
	/**
	 * 
	 * 
	 * @param key
	 * @param list
	 */
	public void addList( String key, List<String> list) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (list != null && !list.isEmpty()) {
				for (String value : list) {
					jedis.rpush(key, value);
				}
			}
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	public List<String> getList(String key) {
		List<String> lists = null;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			lists = jedis.lrange(key, 0, -1);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

		return lists;
	}
	public void addMapItem(String key, String field,String value) {
		if(key==null||field==null||value==null)
			return;
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			Map<String,String> hash = new HashMap<String,String>();
			hash.put(field, value);
			jedis.hmset(key, hash);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public long delMapItem(String key, String field) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.hdel(key, field);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return 0;
	}
	
	
	/**
	 * 统一session使用
	 * @param key
	 * @return
	 */
	public Object getSession(String key) {
		Jedis jedis = null;
		byte[] data = null;
		try {
			jedis = pool.getResource();
			data = jedis.get(key.getBytes());
			return SerializeUtil.unserialize(data);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException){
				if(!createPool())
					return RedisDowntime.getInstance();
			}
			return null;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	/**
	 * 统一session使用
	 * @param key
	 * @return
	 */
	public Object getSession(int dbIndex,String key) {
		Jedis jedis = null;
		byte[] data = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			data = jedis.get(key.getBytes());
			return SerializeUtil.unserialize(data);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException){
				if(!createPool())
					return RedisDowntime.getInstance();
			}
			return null;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}

	}
	
	public boolean exists(String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.exists(key);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return false;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	
	public boolean exists(int dbIndex,String key) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			jedis.select(dbIndex);
			return jedis.exists(key);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return false;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	
	public void addMap(String key, Map<String,String> map, int seconds) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (map != null && !map.isEmpty()) {
				for (String mapkey:map.keySet()) {
					jedis.hset(key, mapkey, map.get(mapkey));
				}
			}
			jedis.expire(key, seconds);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public Set<String> getNUlls(){
		
		
		Jedis jedis = null;
		Set<String> res = new HashSet<String>();
		try {
			jedis = pool.getResource();
			Set<String> keys = jedis.keys("*.css*");
			System.out.println("111111:"+keys.size());
			if(keys != null){
				for(String key : keys){
					String value = jedis.get(key);
					System.out.println(key+"="+value.trim().length());
					if(value == null || value.trim().length() == 0)
						res.add(key);
				}
			}
			return res;
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
			return null;
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}
	
	public void addSet(String key, Set<String> set, int seconds) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			if (set != null && !set.isEmpty()) {
				for (String value : set) {
					jedis.sadd(key, value);
				}
				jedis.expire(key, seconds);
			}
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

	public boolean addSetItem(String key, String itemValue) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.sadd(key, itemValue)==1;
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return false;
	}

	public boolean remSetItem(String key, String itemValue) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.srem(key, itemValue)==1;
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return false;
	}

	public boolean checkSetItemExist(String key, String itemValue) {
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
			return jedis.sismember(key, itemValue);
		} catch (Exception e) {
			log.error("",e);
			if(e instanceof java.net.SocketException ||e instanceof java.net.ConnectException 
					|| e instanceof redis.clients.jedis.exceptions.JedisConnectionException)
				createPool();
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
		return false;

	}


}
