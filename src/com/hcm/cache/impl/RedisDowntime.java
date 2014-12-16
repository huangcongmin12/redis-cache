 package com.hcm.cache.impl;

/**
 *redis宕机   标识类
 */
public class RedisDowntime {
	private static RedisDowntime instance = new  RedisDowntime();
	private RedisDowntime(){
		
	}
	public static RedisDowntime  getInstance(){
		return instance;
	}

}
