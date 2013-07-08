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
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import asyncnode.App;
import asyncnode.ElementBase;
import asyncnode.ElementStatusEnum;
import asyncnode.ExceptionHandler;
import asyncnode.implement.cassandra.CassandraHelper;
import asyncnode.implement.cassandra.CassandraHelper.CassandraDescrib;
import asyncnode.core.IAction2;
import asyncnode.core.IFunc;

@Singleton
public class StoreableElementDao {		
	protected MemoryAsyncNodeDao memoryAsyncNodeDao = null;
	protected MemoryAsyncQueueDao memoryAsyncQueueDao = null;	
	protected CassandraAsyncNodeDao cassandraAsyncNodeDao = null;
	protected CassandraAsyncQueueDao cassandraAsyncQueueDao = null;
	protected CassandraWorkFlowDao cassandraWorkFlowDao = null;
		
	@Inject
	public void setMemoryAsyncNodeDao(MemoryAsyncNodeDao MemoryAsyncNodeDao) {
		this.memoryAsyncNodeDao = MemoryAsyncNodeDao;
	}

	@Inject
	public void setMemoryAsyncQueueDao(MemoryAsyncQueueDao memoryAsyncQueueDao) {
		this.memoryAsyncQueueDao = memoryAsyncQueueDao;
	}

	@Inject
	public void setCassandraAsyncNodeDao(CassandraAsyncNodeDao CassandraAsyncNodeDao) {
		this.cassandraAsyncNodeDao = CassandraAsyncNodeDao;
	}

	@Inject
	public void setCassandraAsyncQueueDao(
			CassandraAsyncQueueDao cassandraAsyncQueueDao) {
		this.cassandraAsyncQueueDao = cassandraAsyncQueueDao;
	}

	@Inject
	public void setCassandraWorkFlowDao(CassandraWorkFlowDao cassandraWorkFlowDao) {
		this.cassandraWorkFlowDao = cassandraWorkFlowDao;
	}
	
	public void initAllStorage() throws Exception
	{			
		try
		{
			restoreQueue();
		}
		catch(Exception ex)
		{
			App.getConfig().getInstance(ExceptionHandler.class).handleException(ex);
		}
	}
	
	protected void restoreAsyncNode() throws Exception
	{
		List<ElementBase> elementBaseList = cassandraAsyncNodeDao.getAll();
		App.getConfig().getInstance(Logger.class).info("..........load " + elementBaseList.size() + " nodes from store!");
		memoryAsyncNodeDao.clearAll();
		for(ElementBase elementBase : elementBaseList)
		{
			elementBase.doStartupResotre();
			memoryAsyncNodeDao.add(elementBase);
		}
	}
	
	protected void restoreQueue() throws Exception
	{		
		List<String> nodeList = new ArrayList<String>();		
		ConcurrentMap<Integer, Set<String[]>> asyncQueue = cassandraAsyncQueueDao.load();
		memoryAsyncQueueDao.setAsyncQueue(asyncQueue);
		restoreAsyncNode();
	}

	
	public Object getAttribute(String elementID) throws Exception {
		return memoryAsyncNodeDao.getValue(elementID);
	}

	public ElementBase getElementBase(String elementID) throws Exception
	{
		return memoryAsyncNodeDao.get(elementID);
	}
	
	protected void syncElementStatus(ElementBase elementBase) throws Exception
	{
		ElementStatusEnum status = elementBase.getStatus();
		if(status.equals(ElementStatusEnum.Pending))
		{
			syncElementStatus(elementBase, null, ElementStatusEnum.Pending);
		}
		else if(status.equals(ElementStatusEnum.Ready))
		{
			syncElementStatus(elementBase, ElementStatusEnum.Pending, ElementStatusEnum.Ready);
		}
		else if(status.equals(ElementStatusEnum.Running))
		{
			syncElementStatus(elementBase, ElementStatusEnum.Ready, ElementStatusEnum.Running);
		}
		else if(status.equals(ElementStatusEnum.NotifyFinish))
		{
			syncElementStatus(elementBase, ElementStatusEnum.Running, ElementStatusEnum.NotifyFinish);
		}		
		else if(status.equals(ElementStatusEnum.NotifiedOutput))
		{
			syncElementStatus(elementBase, ElementStatusEnum.NotifyFinish, ElementStatusEnum.NotifiedOutput);
		}
		else if(status.equals(ElementStatusEnum.NotifiedInput))
		{
			syncElementStatus(elementBase, ElementStatusEnum.NotifiedOutput, ElementStatusEnum.NotifiedInput);
		}
		else if(status.equals(ElementStatusEnum.Destryable))
		{
			syncElementStatus(elementBase, ElementStatusEnum.NotifiedInput, ElementStatusEnum.Destryable);
		}
		else if(status.equals(ElementStatusEnum.Destryed))
		{
			syncElementStatus(elementBase, ElementStatusEnum.Destryable, ElementStatusEnum.Destryed);
		}		
	}
	
	protected void removeElementQueue(Integer priority, String elementID) throws Exception
	{
		cassandraAsyncQueueDao.forceRemove(priority, elementID);
		memoryAsyncQueueDao.forceRemove(priority, elementID);
	}
	
	public void schduleOneElement(Integer priority, String elementID) throws Exception
	{
		ElementBase elementBase = getElementBase(elementID);
		if(elementBase == null)
		{
			removeElementQueue(priority, elementID);			
			App.getConfig().getInstance(Logger.class).warn("force remove element which is null from queue:" + elementID);
			return;
		}	
		elementBase.schdule();		
	}
	
	
	public boolean runByPriority() throws Exception
	{	
		try
		{
			memoryAsyncQueueDao.moveBufferQueue();
			memoryAsyncNodeDao.moveBufferElement();
			
			if(memoryAsyncQueueDao.isActiveEmpty())
			{			
				return false;
			}
			Map<Integer, String[]> currentList = memoryAsyncQueueDao.getCurrentRunableList();
			for(Entry<Integer, String[]> nodeEntry : currentList.entrySet())
			{
				Integer priority = nodeEntry.getKey();
				for(String node : nodeEntry.getValue())
				{
					this.schduleOneElement(priority, node);
				}
			}
			return true;
		}
		catch(Exception ex)
		{
			App.getConfig().getInstance(ExceptionHandler.class).handleException(ex);
			return false;
		}
	}

	public void syncElementStatus(ElementBase elementBase,
			ElementStatusEnum oldStatus, ElementStatusEnum newStatus) throws Exception
	{
		if(elementBase.isPersistent())
		{
			cassandraWorkFlowDao.swapStatus(elementBase.getWorkFlowID().toString(), 
					elementBase.getId().toString(), 
					oldStatus, newStatus);		
		}
	}
	
	public boolean safeRetryableChangeStatus(ElementBase elementBase, 
			ElementStatusEnum oldStatus,
			ElementStatusEnum newStatus,
			List<String> columnList) throws Exception
	{
		if(elementBase.getStatus().equals(oldStatus))
		{
			elementBase.setStatus(newStatus);
			updateStatus(elementBase, columnList);
			return true;
		}
		else
		{
			return false;
		}
	}
	
	
	public void addElement(ElementBase element) throws Exception {
		try
		{
			ElementStatusEnum oldStatus = null;
			if(element.getInNodesMap().size() > 0 && !isAllInputNodesFinished(element))
			{
				oldStatus = element.getStatus();
				element.setStatus(ElementStatusEnum.Pending);
				
				if(element.isPersistent())
				{
					addCassandraElementQueue(element);
				
					cassandraWorkFlowDao.swapStatus(element.getWorkFlowID().toString(), 
							element.getId().toString(), 
							null, ElementStatusEnum.Pending);		
				}
			}
			else
			{
				oldStatus = element.getStatus();
				element.setStatus(ElementStatusEnum.Ready);
				
				if(element.isPersistent())
				{
					addCassandraElementQueue(element);
					
					cassandraWorkFlowDao.swapStatus(element.getWorkFlowID().toString(), 
							element.getId().toString(), 
							null, ElementStatusEnum.Ready);		
				}
			}
			
			ElementBase.checkNameValidate(element.getId().toString());
			if(element.isPersistent())
			{
				cassandraAsyncNodeDao.add(element);
			}
			
			App.getConfig().getInstance(Logger.class).info("setPending!" + element.getClassName()+"," + element.getWorkFlowID()+ "," + element.getId());
		}
		catch(Exception ex)
		{
			App.getConfig().getInstance(ExceptionHandler.class).handleException(ex);
		}
	}
	
	public void addBufferElementList(List<ElementBase> elementList) throws Exception {
		memoryAsyncQueueDao.addBufferList(elementList);
		memoryAsyncNodeDao.addBufferList(elementList);
	}
	
	public void submitAppVariables(ElementBase element, ConcurrentMap<String, Object> varMap) throws Exception
	{
		element.setLastEditDate(new Date());
		List<String> columnList = new ArrayList<String>();			
		columnList.add("lastEditDate");
		columnList.add("appVariables");
		Map<String, Object> columnValueHash = new HashMap<String, Object>();
		columnValueHash.put("appVariables", varMap);
		if(element.isPersistent())
		{
			cassandraAsyncNodeDao.update(element, columnList, columnValueHash);
		}
		memoryAsyncNodeDao.update(element, columnList, columnValueHash);
	}
	
	protected void updateStatus(ElementBase element, List<String> affectedFields) throws Exception
	{
		element.setLastEditDate(new Date());
		List<String> columnList = new ArrayList<String>();
		columnList.add("lastEditDate");
		columnList.add("status");
		if(affectedFields != null && !affectedFields.isEmpty())
		{
			columnList.addAll(affectedFields);
		}
		if(element.isPersistent())
		{
			cassandraAsyncNodeDao.update(element, columnList, null);
		}
		memoryAsyncNodeDao.update(element, columnList, null);
		syncElementStatus(element);
	}
	
	
	public void setReady(ElementBase element)  throws Exception {
		if(isAllInputNodesFinished(element))
		{
			ElementStatusEnum oldStatus = element.getStatus();
			element.setStatus(ElementStatusEnum.Ready);
			addElementQueue(oldStatus, element);
			updateStatus(element, null);
			removeElementQueue(oldStatus, element);
			App.getConfig().getInstance(Logger.class).info("setReady!");
		}
		else
		{
			ElementStatusEnum oldStatus = element.getStatus();
			if(oldStatus.equals(ElementStatusEnum.Pending))
			{
				removeElementQueue(oldStatus, element);
			}
		}
	}
	
	protected void addElementQueue(ElementStatusEnum oldQueueStatus, ElementBase element) throws Exception
	{
		ElementStatusEnum newStatus = element.getStatus();
		if(!(newStatus.equals(ElementStatusEnum.Pending) 
				|| newStatus.equals(ElementStatusEnum.Running)
				|| newStatus.equals(ElementStatusEnum.NotifiedInput)
				|| newStatus.equals(ElementStatusEnum.Destryed)))
		{
			App.getConfig().getInstance(Logger.class).info("add queue:" + element.getStatus() + "," + element.getClass().getName());
			if(element.isPersistent())
			{
				cassandraAsyncQueueDao.add(element.getStatus(), element);
			}
			memoryAsyncQueueDao.add(element.getStatus(), element);
		}
	}
	
	protected void addCassandraElementQueue(ElementBase element) throws Exception
	{
		ElementStatusEnum newStatus = element.getStatus();
		if(!(newStatus.equals(ElementStatusEnum.Pending) 
				|| newStatus.equals(ElementStatusEnum.Running)
				|| newStatus.equals(ElementStatusEnum.NotifiedInput)
				|| newStatus.equals(ElementStatusEnum.Destryed)))
		{
			App.getConfig().getInstance(Logger.class).info("add queue:" + element.getStatus() + "," + element.getClass().getName());
			if(element.isPersistent())
			{
				cassandraAsyncQueueDao.add(element.getStatus(), element);
			}
		}
	}

	
	protected void removeElementQueue(ElementStatusEnum oldQueueStatus, ElementBase element) throws Exception
	{
		if(element.isPersistent())
		{
			cassandraAsyncQueueDao.remove(oldQueueStatus, element);
		}
		memoryAsyncQueueDao.remove(oldQueueStatus, element);
	}
	
	
	public void setRunning(ElementBase element)  throws Exception {
				
		ElementStatusEnum oldStatus = element.getStatus();
		element.setStatus(ElementStatusEnum.Running);
		addElementQueue(oldStatus, element);
		
		updateStatus(element, null);
		removeElementQueue(oldStatus, element);
		App.getConfig().getInstance(Logger.class).info("setRunning!" + element.getClassName());
	}

	
	public void setNotifyFinish(ElementBase element)  throws Exception {		
		try
		{
			ElementStatusEnum oldStatus = element.getStatus();
			element.setStatus(ElementStatusEnum.NotifyFinish);
			addElementQueue(oldStatus, element);
			List<String> columnList = new ArrayList<String>();
			columnList.add("value");
			updateStatus(element, columnList);
			removeElementQueue(oldStatus, element);
			App.getConfig().getInstance(Logger.class).info("setNotifyFinish!");
		}
		catch(Exception ex)
		{
			App.getConfig().getInstance(ExceptionHandler.class).handleException(ex);			
		}
	}

	protected void notifyAllOutNodeInputFinished(ElementBase element) throws Exception
	{
		for(Entry<String, Boolean> outEntry : element.getOutNodesMap().entrySet())
		{
			ElementBase outElement;

			outElement = getElementBase(outEntry.getKey());
			if(outElement == null)
			{
				continue;
			}
			ElementBase.checkNameValidate(element.getId().toString());
			outElement.getInNodesMap().put(element.getId().toString(), true);
			outElement.setLastEditDate(new Date());
			List<String> columnList = new ArrayList<String>();			
			columnList.add("lastEditDate");
			columnList.add("inNodesMap");
			Map<String, Object> columnValueHash = new HashMap<String, Object>();
			columnValueHash.put("inNodesMap", element.getId().toString());
			if(outElement.isPersistent())
			{
				cassandraAsyncNodeDao.update(outElement, columnList, columnValueHash);
			}
			memoryAsyncNodeDao.update(outElement, columnList, columnValueHash);
			
			handleSingleOutNodeReadyEvent(outElement);
		}
	}
	
	protected Boolean isAllInputNodesFinished(ElementBase outElement)
	{
		Boolean inputAllFinished = true;
		for(Entry<String, Boolean> inEntry : outElement.getInNodesMap().entrySet())
		{
			if(!inEntry.getValue())
			{
				inputAllFinished = false;
				break;
			}
		}
		App.getConfig().getInstance(Logger.class).info("find one node all input finished:" + outElement.getClass().getName()+",status:" + outElement.getStatus());
		return inputAllFinished;
	}
	
	protected boolean handleSingleOutNodeReadyEvent(ElementBase outElement)  throws Exception
	{
		Boolean inputAllFinished = isAllInputNodesFinished(outElement);	
		if(inputAllFinished && outElement.getStatus().equals(ElementStatusEnum.Pending))
		{
			if(outElement.isPersistent())
			{
				cassandraAsyncQueueDao.add(outElement.getStatus(), outElement);
			}
			memoryAsyncQueueDao.add(outElement.getStatus(), outElement);
			return true;
		}
		else
		{
			return false;
		}
	}
	
	
	public void setNotifiedOutput(ElementBase element)  throws Exception {
		notifyAllOutNodeInputFinished(element);			

		ElementStatusEnum oldStatus = element.getStatus();
		element.setStatus(ElementStatusEnum.NotifiedOutput);
		addElementQueue(oldStatus, element);
		
		updateStatus(element, null);
		removeElementQueue(oldStatus, element);
		App.getConfig().getInstance(Logger.class).info("setNotifiedOutput!");
	}
	
	protected void saveOutNodesChange(ElementBase inElement, Object[][] outElementArray) throws Exception
	{
		inElement.setLastEditDate(new Date());			
		List<String> columnList = new ArrayList<String>();
		columnList.add("lastEditDate");
		columnList.add("outNodesMap");			
		Map<String, Object> columnValueHash = new HashMap<String, Object>();
		columnValueHash.put("outNodesMap", outElementArray);			
		if(inElement.isPersistent())
		{
			cassandraAsyncNodeDao.update(inElement, columnList, columnValueHash);
		}
		memoryAsyncNodeDao.update(inElement, columnList, columnValueHash);
	}

	protected void notifyAllInNodeDestroyable(ElementBase element) throws Exception
	{
		for(Entry<String, Boolean> inEntry : element.getInNodesMap().entrySet())
		{
			ElementBase inElement;

			inElement = getElementBase(inEntry.getKey());
			if(inElement == null)
			{
				continue;
			}
			ElementBase.checkNameValidate(element.getId().toString());
			inElement.getOutNodesMap().put(element.getId().toString(), true);
			
			saveOutNodesChange(inElement, new Object[][]{ new Object[] { element.getId().toString(), true }});
			
			handleSingleInNodeDestroyableEvent(inElement);
		}
	}	
	
	protected boolean isAllOutNodesFinish(ElementBase inElement)
	{
		Boolean outputAllFinished = true;
		for(Entry<String, Boolean> outEntry : inElement.getOutNodesMap().entrySet())
		{
			if(!outEntry.getValue())
			{
				outputAllFinished = false;
				break;
			}
		}
		return outputAllFinished;
	}

	protected boolean handleSingleInNodeDestroyableEvent(ElementBase inElement)  throws Exception
	{
		Boolean outputAllFinished = isAllOutNodesFinish(inElement);
		if(outputAllFinished  && inElement.getStatus().equals(ElementStatusEnum.NotifiedInput))
		{
			if(inElement.isPersistent())
			{
				cassandraAsyncQueueDao.add(inElement.getStatus(), inElement);
			}
			memoryAsyncQueueDao.add(inElement.getStatus(), inElement);
			App.getConfig().getInstance(Logger.class).info("can notify finished!" + inElement.getClass().getName());
			return true;
		}
		else
		{
			return false;
		}
	}

	
	public void setNotifiedInput(ElementBase element)  throws Exception {
		notifyAllInNodeDestroyable(element);			
		
		ElementStatusEnum oldStatus = element.getStatus();
		element.setStatus(ElementStatusEnum.NotifiedInput);
		addElementQueue(oldStatus, element);
		handleSingleInNodeDestroyableEvent(element);
		updateStatus(element, null);
		removeElementQueue(oldStatus, element);
		
		App.getConfig().getInstance(Logger.class).info("setNotifiedInput!");
	}

	
	public void setDestryable(ElementBase element)  throws Exception {
		if(isAllOutNodesFinish(element))
		{
			ElementStatusEnum oldStatus = element.getStatus();
			element.setStatus(ElementStatusEnum.Destryable);
			addElementQueue(oldStatus, element);
			
			updateStatus(element, null);
			removeElementQueue(oldStatus, element);
			App.getConfig().getInstance(Logger.class).info("setDestryable!");
		}
		else
		{
			ElementStatusEnum oldStatus = element.getStatus();
			if(oldStatus.equals(ElementStatusEnum.NotifiedInput))
			{
				removeElementQueue(oldStatus, element);
			}
		}
	}
	
	
	public void setDestryed(ElementBase element)  throws Exception {
		ElementStatusEnum oldStatus = element.getStatus();
		
		String workFlowID = element.getWorkFlowID().toString();
		String nodeID = element.getId().toString();
		if(element.isPersistent())
		{
			cassandraWorkFlowDao.delete(workFlowID, nodeID);
			cassandraAsyncNodeDao.delete(nodeID);
		}
		memoryAsyncNodeDao.delete(nodeID);
		element.setStatus(ElementStatusEnum.Destryed);
		removeElementQueue(oldStatus, element);
		App.getConfig().getInstance(Logger.class).info("delete node!!" + element.getClassName());
	}
}