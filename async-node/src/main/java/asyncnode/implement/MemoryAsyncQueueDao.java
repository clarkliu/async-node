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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.inject.Singleton;

import asyncnode.AsyncNodeRunner;
import asyncnode.ElementBase;
import asyncnode.ElementStatusEnum;

@Singleton
public class MemoryAsyncQueueDao {
	protected int counter = 1;
	
	protected ConcurrentMap<Integer, Set<String[]>> asyncQueue = new ConcurrentHashMap<Integer, Set<String[]>>();
	protected ConcurrentMap<Integer, Set<String[]>> pendingAsyncQueue = new ConcurrentHashMap<Integer, Set<String[]>>();
	
	public synchronized ConcurrentMap<Integer, Set<String[]>> getAsyncQueue()
	{
		return asyncQueue;
	}
	
	public synchronized ConcurrentMap<Integer, Set<String[]>> getPendingAsyncQueue()
	{
		return pendingAsyncQueue;
	}
	
	public synchronized void setAsyncQueue(ConcurrentMap<Integer, Set<String[]>> asyncQueue)
	{
		this.asyncQueue = asyncQueue;
	}
	
	public synchronized void addBufferList(List<ElementBase> elementList) throws Exception
	{
		for(ElementBase element : elementList)
		{
			ElementStatusEnum newStatus = element.getStatus();
			if(!(newStatus.equals(ElementStatusEnum.Pending) 
					|| newStatus.equals(ElementStatusEnum.Running)
					|| newStatus.equals(ElementStatusEnum.NotifiedInput)
					|| newStatus.equals(ElementStatusEnum.Destryed)))
			{
				add(this.pendingAsyncQueue, element.getStatus(), element);
			}		
		}
	}
	
	public synchronized void moveBufferQueue()
	{
		for(Entry<Integer, Set<String[]>> entry : pendingAsyncQueue.entrySet())
		{
			if(!asyncQueue.containsKey(entry.getKey()))
			{
				asyncQueue.put(entry.getKey(), entry.getValue());
			}
			else
			{
				Set<String[]> strSet = asyncQueue.get(entry.getKey());
				strSet.addAll(entry.getValue());
			}
		}
		pendingAsyncQueue.clear();
	}	
	
	protected synchronized boolean isMapEmpty(ConcurrentMap<Integer, Set<String[]>> map)
	{
		for(Entry<Integer, Set<String[]>> entry : map.entrySet())
		{
			if(!entry.getValue().isEmpty())
			{
				return false;
			}
		}
		return true;
	}	
	
	public synchronized boolean isActiveEmpty()
	{
		return isMapEmpty(this.asyncQueue) && isMapEmpty(this.pendingAsyncQueue);
	}
	
	public synchronized Map<Integer, String[]> getCurrentRunableList()
	{		
		Map<Integer, String[]> result = new HashMap<Integer, String[]>();
		for(Entry<Integer, Set<String[]>> entry : this.asyncQueue.entrySet())
		{
			if(counter % entry.getKey() == 0)
			{
				Set<String[]> statusValueSet = entry.getValue();
				String[] valueArray = new String[statusValueSet.size()];
				int valueI = 0;
				for(String[] statusValue : statusValueSet)
				{
					valueArray[valueI] = statusValue[0];
					valueI++;
				}
				result.put(entry.getKey(), valueArray);
			}
		}
		
		if(counter < Integer.MAX_VALUE)
		{
			counter++;
		}
		else
		{
			counter = 1;
		}
		return result;
	}
		
	protected synchronized void add(ConcurrentMap<Integer, Set<String[]>> map, ElementStatusEnum status, ElementBase element) throws Exception	
	{
		Integer priority = AsyncNodeRunner.getSchduleCycle(status, element);
		String nodeID = element.getId().toString();		
		Set<String[]> queue = null;
		if(map.containsKey(priority))
		{
			queue = map.get(priority);
		}
		else
		{
			queue = new HashSet<String[]>();
			map.put(priority, queue);
		}
		queue.add(new String[] { nodeID,
					CassandraWorkFlowDao.getStatus(status) } );
	}
	
	public synchronized void add(ElementStatusEnum status, ElementBase element) throws Exception {
		add(this.asyncQueue, status, element);
	}

	
	public synchronized void remove(ElementStatusEnum status, ElementBase element) throws Exception {
		Integer priority = AsyncNodeRunner.getSchduleCycle(status, element);
		String nodeID = element.getId().toString();	

		if(this.asyncQueue.containsKey(priority))
		{
			Set<String[]> queue = this.asyncQueue.get(priority);
			List<String[]> selectedNodeList = new ArrayList<String[]>();
			String statusPrefix = CassandraWorkFlowDao.getStatus(status);
			for(String[] statusNode : queue)
			{
				if(statusNode[0].equalsIgnoreCase(nodeID) 
						&& statusNode[1].equalsIgnoreCase(statusPrefix))
				{
					selectedNodeList.add(statusNode);
				}
			}
			if(selectedNodeList != null && selectedNodeList.size() > 0)
			{
				for(String[] selectedNode : selectedNodeList)
				{
					queue.remove(selectedNode);
				}
			}
			if(queue.isEmpty())
			{
				this.asyncQueue.remove(priority);
			}
		}
	}

	
	public synchronized void forceRemove(Integer priority, String nodeID) throws Exception {
		
		if(this.asyncQueue.containsKey(priority))
		{
			Set<String[]> queue = this.asyncQueue.get(priority);
			List<String[]> selectedNodeList = new ArrayList<String[]>();
			for(String[] statusNode : queue)
			{
				if(statusNode[0].equalsIgnoreCase(nodeID))
				{
					selectedNodeList.add(statusNode);
				}
			}
			if(selectedNodeList != null && selectedNodeList.size() > 0)
			{
				for(String[] selectedNode : selectedNodeList)
				{
					queue.remove(selectedNode);
				}
			}
			if(queue.isEmpty())
			{
				this.asyncQueue.remove(priority);
			}
		}
	}
}