package com.hcm.redis.test;

public class MainTest {
	
	public void show() {
		
//		StringOperate();
//		ListOperate();
//		SetOperate();
//		SortedSetOperate();
//		HashOperate();
//		jedisPool.returnResource(jedis);
//		shardedJedisPool.returnResource(shardedJedis);
	}
	
	public static void main(String[] args) {
		RedisClient redis = new RedisClient();
		redis.KeyOperate();
	}
}
