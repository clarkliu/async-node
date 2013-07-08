//   Copyright 2013 Clark Liu
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package asyncnode.implement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.inject.Singleton;

import asyncnode.ElementBase;
import asyncnode.ElementStatusEnum;
import asyncnode.implement.cassandra.CassandraHelper;
import asyncnode.implement.cassandra.CassandraHelper.CassandraDescrib;
import asyncnode.core.IAction2;

import asyncnode.core.IFunc;

@Singleton
public class MemoryAsyncNodeDao
{
	protected ConcurrentMap<String, ElementBase> elementBaseMap = new ConcurrentHashMap<String, ElementBase>();	
	protected ConcurrentMap<String, ElementBase> pendingElementBaseMap = new ConcurrentHashMap<String, ElementBase>();
	
	public ConcurrentMap<String, ElementBase> getElementBaseMap()
	{
		return elementBaseMap;
	}
	
	public ConcurrentMap<String, ElementBase> getPendingElementBaseMap()
	{
		return pendingElementBaseMap;
	}
	
	public synchronized void addBufferList(List<ElementBase> elementList) throws Exception
	{
		for(ElementBase elementBase : elementList)
		{
			pendingElementBaseMap.put(elementBase.getId().toString(), elementBase);
		}
	}

	public synchronized void moveBufferElement()
	{
		for(Entry<String, ElementBase> entry : pendingElementBaseMap.entrySet())
		{
			elementBaseMap.put(entry.getKey(), entry.getValue());
		}
		
		pendingElementBaseMap.clear();
	}
		
	public synchronized void clearAll()
	{
		elementBaseMap = new ConcurrentHashMap<String, ElementBase>();
	}
	
	public synchronized void delete(String key) throws Exception
	{
		elementBaseMap.remove(key);
	}
		
	public synchronized ElementBase get(String key) throws Exception
	{
		if(!elementBaseMap.containsKey(key))
		{
			return null;
		}
		return elementBaseMap.get(key);		
	}

		
	public synchronized Object getValue(String key) throws Exception
	{
		ElementBase elementBase = get(key);
		if(elementBase == null)
		{
			return null;
		}
		return elementBase.value;
	}
	
	public synchronized void add(final ElementBase elementBase) throws Exception
	{
		elementBaseMap.put(elementBase.getId().toString(), elementBase);
	}

	public synchronized void update(final ElementBase elementBase, List<String> columnList, Map<String, Object> columnValueHash) throws Exception
	{
		elementBaseMap.put(elementBase.getId().toString(), elementBase);
	}	
}
