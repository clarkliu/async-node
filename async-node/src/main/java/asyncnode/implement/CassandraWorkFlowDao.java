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
import org.codehaus.jackson.map.ObjectMapper;

import com.google.inject.Singleton;

import asyncnode.ElementBase;
import asyncnode.ElementStatusEnum;
import asyncnode.implement.cassandra.CassandraHelper;
import asyncnode.implement.cassandra.CassandraHelper.CassandraDescrib;
import asyncnode.core.IAction2;

import asyncnode.core.IFunc;

@Singleton
public class CassandraWorkFlowDao
{	
	public static final String PendingPrefix = "PI";
	public static final String ReadyPrefix = "RD";
	public static final String RunningPrefix = "RI";
	public static final String NotifyFinishPrefix = "NF";
	public static final String NotifiedOutputPrefix = "NO";
	public static final String NotifiedInputPrefix = "NI";
	public static final String DestryablePrefix = "DA";
	public static final String DestryedPrefix = "DD";
	public static final int PrefixLength = 2;	
	
	protected CassandraDescrib cassandraDescrib = null;
	public void setCassandraDescrib(CassandraDescrib cassandraDescrib)
	{
		this.cassandraDescrib = cassandraDescrib;
	}
	
	public CassandraDescrib getDescrib()
	{
		return cassandraDescrib;
	}
	
	public String getNodeID(String status)
	{
		return status.substring(PrefixLength + 1);
	}
	
	public ElementStatusEnum getStatus(String status)
	{
		ElementStatusEnum eStatus = null;
		status = status.substring(0, PrefixLength).toUpperCase();
		if(status.equals(PendingPrefix))
		{
			eStatus = ElementStatusEnum.Pending;
		}
		else if(status.equals(ReadyPrefix))
		{
			eStatus = ElementStatusEnum.Ready;
		}
		else if(status.equals(RunningPrefix))
		{
			eStatus = ElementStatusEnum.Running;
		}
		else if(status.equals(NotifyFinishPrefix))
		{
			eStatus = ElementStatusEnum.NotifyFinish;
		}		
		else if(status.equals(NotifiedOutputPrefix))
		{
			eStatus = ElementStatusEnum.NotifiedOutput;
		}
		else if(status.equals(NotifiedInputPrefix))
		{
			eStatus = ElementStatusEnum.NotifiedInput;
		}
		else if(status.equals(DestryablePrefix))
		{
			eStatus = ElementStatusEnum.Destryable;
		}
		else if(status.equals(DestryedPrefix))
		{
			eStatus = ElementStatusEnum.Destryed;
		}
		return eStatus;
	}
	
	public static String getStatus(ElementStatusEnum status)
	{
		String sStatus = null;
		if(status.equals(ElementStatusEnum.Pending))
		{
			sStatus = PendingPrefix;
		}
		else if(status.equals(ElementStatusEnum.Ready))
		{
			sStatus = ReadyPrefix;
		}
		else if(status.equals(ElementStatusEnum.Running))
		{
			sStatus = RunningPrefix;
		}
		else if(status.equals(ElementStatusEnum.NotifyFinish))
		{
			sStatus = NotifyFinishPrefix;
		}		
		else if(status.equals(ElementStatusEnum.NotifiedOutput))
		{
			sStatus = NotifiedOutputPrefix;
		}
		else if(status.equals(ElementStatusEnum.NotifiedInput))
		{
			sStatus = NotifiedInputPrefix;
		}
		else if(status.equals(ElementStatusEnum.Destryable))
		{
			sStatus = DestryablePrefix;
		}
		else if(status.equals(ElementStatusEnum.Destryed))
		{
			sStatus = DestryedPrefix;
		}
		return sStatus;
	}
	
	protected List<Mutation> getAddStatusMutation(String nodeID, 
			ElementStatusEnum status)
	{
		if(status == null)
		{
			return null;
		}
		Map<String, String> columnHash = new HashMap<String, String>();
		String columnName = getStatus(status) + "_" + nodeID;
		columnHash.put(columnName, null);
		List<Mutation> mutationList = CassandraHelper.getDynamicMutationList(columnHash);
		return mutationList;
	}
	
	protected Mutation getDeleteStatusMutation(String nodeID, 
			ElementStatusEnum status)
	{
		if(status == null)
		{
			return null;
		}
		String columnName = getStatus(status) + "_" + nodeID;
		Mutation mutation = CassandraHelper.getDeleteMutation(columnName.getBytes());
		return mutation;
	}	
	
	
	public void delete(String workFlowID) throws Exception
	{
		CassandraHelper.deleteRow(cassandraDescrib, workFlowID.getBytes());
	}
	
	
	public void delete(String workFlowID, String nodeID) throws Exception
	{
		List<Mutation> mutationList = new ArrayList<Mutation>();
		Mutation deleteMutation = getDeleteStatusMutation(nodeID, ElementStatusEnum.Destryable);
		if(deleteMutation != null)
		{
			mutationList.add(deleteMutation);
		}		
		Map<ByteBuffer,List<Mutation>> valueMap = CassandraHelper.getMutationMap(workFlowID, mutationList);
		CassandraHelper.batchUpdate(cassandraDescrib, valueMap , ConsistencyLevel.ONE);
	}
	
		
	public void swapStatus(String workFlowID, String nodeID, 
			ElementStatusEnum oldStatus, ElementStatusEnum newStatus) throws Exception
	{
		List<Mutation> mutationList = new ArrayList<Mutation>();
		Mutation deleteMutation = getDeleteStatusMutation(nodeID, oldStatus);
		List<Mutation> addMutationList = getAddStatusMutation(nodeID, newStatus);
		if(deleteMutation != null)
		{
			mutationList.add(deleteMutation);
		}
		
		if(addMutationList != null)
		{
			mutationList.addAll(addMutationList);
		}
		Map<ByteBuffer,List<Mutation>> valueMap = CassandraHelper.getMutationMap(workFlowID, mutationList);
		CassandraHelper.batchUpdate(cassandraDescrib, valueMap , ConsistencyLevel.ONE);
	}

		
	public List<String> getStatusList(String workFlowID, ElementStatusEnum status, 
			int listCount) throws Exception
	{
		String statusPrefix = getStatus(status);
		List<ColumnOrSuperColumn> columnList = CassandraHelper.getPrefixColumns(cassandraDescrib, workFlowID, statusPrefix, listCount);
		List<String> resultList = new ArrayList<String>();
		if(columnList != null)
		{
			for(ColumnOrSuperColumn column : columnList)
			{
				String id = new String(column.column.getName()).substring(statusPrefix.length() + 1);
				resultList.add(id);
			}
		}
		return resultList;
	}
}
