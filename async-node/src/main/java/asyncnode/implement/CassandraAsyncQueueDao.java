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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;

import com.google.inject.Singleton;

import asyncnode.AsyncNodeRunner;
import asyncnode.ElementBase;
import asyncnode.ElementStatusEnum;
import asyncnode.implement.cassandra.CassandraHelper;
import asyncnode.implement.cassandra.CassandraHelper.CassandraDescrib;
import asyncnode.implement.cassandra.FieldSerailzerBase;
import asyncnode.core.IAction2;

@Singleton
public class CassandraAsyncQueueDao {
	protected CassandraDescrib cassandraDescrib = null;	
	public void setCassandraDescrib(CassandraDescrib cassandraDescrib)
	{
		this.cassandraDescrib = cassandraDescrib;
	}
	
	public ConcurrentMap<Integer, Set<String[]>> load() throws Exception
	{
		final ConcurrentMap<Integer, Set<String[]>> result = new ConcurrentHashMap<Integer, Set<String[]>>();
		CassandraHelper.getAllMutation(cassandraDescrib, new IAction2<String, List<String>>(){
			
			public void invoke(String tParam, List<String> param2)
					throws Exception {
				
				Integer key = Integer.parseInt(tParam);
				Set<String[]> columns = new HashSet<String[]>();
				result.put(key, columns);
				for(String statusNode : param2)
				{
					String nodeID = statusNode.substring(0, statusNode.length() - 3);
					String status = statusNode.substring(statusNode.length() - 3 + 1, statusNode.length());
					columns.add(new String[] {
						nodeID,
						status
					});
				}
			}});
		return result;
	}
 	
	
	public void add(ElementStatusEnum status, ElementBase element) throws Exception {
		
		Integer priority = AsyncNodeRunner.getSchduleCycle(status, element);
		String nodeID = element.getId().toString();
		String statusPrefix = CassandraWorkFlowDao.getStatus(status);
		Map<String, String> columnHash = new HashMap<String, String>();
		columnHash.put(nodeID + "_" + statusPrefix, null);
		List<Mutation> mutationList = CassandraHelper.getDynamicMutationList(columnHash);		
		Map<ByteBuffer, List<Mutation>> valueMap = CassandraHelper.getMutationMap(Integer.class, priority, mutationList);
		CassandraHelper.batchUpdate(cassandraDescrib, valueMap, ConsistencyLevel.ONE);
	}

	
	public void remove(ElementStatusEnum status, ElementBase element) throws Exception {
		
		Integer priority = AsyncNodeRunner.getSchduleCycle(status, element);
		String nodeID = element.getId().toString();
		String statusPrefix = CassandraWorkFlowDao.getStatus(status);
		
		Mutation mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + statusPrefix).getBytes());
		List<Mutation> mutationList = new ArrayList<Mutation>();
		mutationList.add(mutation);
		Map<ByteBuffer, List<Mutation>> valueMap = CassandraHelper.getMutationMap(Integer.class, priority, mutationList);
		CassandraHelper.batchUpdate(cassandraDescrib, valueMap, ConsistencyLevel.ONE);
	}

	protected List<Mutation> getAllStatusMutationList(String nodeID)
	{		
		List<Mutation> mutationList = new ArrayList<Mutation>();		
		Mutation mutation = null;
		
		mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + CassandraWorkFlowDao.PendingPrefix).getBytes());
		mutationList.add(mutation);
		
		mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + CassandraWorkFlowDao.ReadyPrefix).getBytes());
		mutationList.add(mutation);
		
		mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + CassandraWorkFlowDao.RunningPrefix).getBytes());
		mutationList.add(mutation);
		
		mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + CassandraWorkFlowDao.NotifyFinishPrefix).getBytes());
		mutationList.add(mutation);
		
		mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + CassandraWorkFlowDao.NotifiedOutputPrefix).getBytes());
		mutationList.add(mutation);
		
		mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + CassandraWorkFlowDao.NotifiedInputPrefix).getBytes());
		mutationList.add(mutation);
		
		mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + CassandraWorkFlowDao.DestryablePrefix).getBytes());
		mutationList.add(mutation);
		
		mutation = CassandraHelper.getDeleteMutation((nodeID + "_" + CassandraWorkFlowDao.DestryedPrefix).getBytes());
		mutationList.add(mutation);
		
		return mutationList;
	}
	
	
	public void forceRemove(Integer priority, String nodeID) throws Exception {
		
		List<Mutation> mutationList = getAllStatusMutationList(nodeID);		
		Map<ByteBuffer, List<Mutation>> valueMap = CassandraHelper.getMutationMap(Integer.class, priority, mutationList);
		CassandraHelper.batchUpdate(cassandraDescrib, valueMap, ConsistencyLevel.ONE);
	}
}
