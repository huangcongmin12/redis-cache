package com.hcm.cache;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: huangcm
 * @date:2014
 */
public interface ICache {
	
	public void addItemToList(String key, Object object);

	public List<Object> getItemFromList(String key);

	public void addItem(String key, Object object);

	public void addItem(String key, Object object, int seconds);

	public String flushDB();

	public Object getItem(String key);

	public long delItem(String key);

	public long getIncrement(String key);

	public long getIncrementBy(String key, long increment);

	public void addMap(String key, Map<String, String> map);

	public void addMap(String key, Map<String, String> map, int seconds);

	public Map<String, String> getMap(String key);

	public String getMapItem(String key, String field);

	public void addMapItem(String key, String field, String value);

	public long delMapItem(String key, String field);

	public long getMapIncrmentBy(String key, String field, long increment);

	public boolean isMapItemExists(String key, String field);

	public void addSet(String key, Set<String> set);

	public void addSet(String key, Set<String> set, int seconds);

	public boolean addSetItem(String key, String itemValue);

	public boolean remSetItem(String key, String itemValue);

	public boolean checkSetItemExist(String key, String itemValue);

	public Set<String> getSet(String key);

	public void addList(String key, List<String> list);

	public List<String> getList(String key);

	public void addItemFile(String key, byte[] file);

	public boolean exists(String key);
}
