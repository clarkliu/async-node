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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.inject.Singleton;

import asyncnode.ElementBase;
import asyncnode.ElementStatusEnum;
import asyncnode.implement.cassandra.CassandraHelper;
import asyncnode.implement.cassandra.CassandraHelper.CassandraDescrib;
import asyncnode.core.IAction2;

import asyncnode.core.IFunc;

@Singleton
public class CassandraAsyncNodeDao
{
	protected CassandraDescrib cassandraDescrib = null;
	public void setCassandraDescrib(CassandraDescrib cassandraDescrib)
	{
		this.cassandraDescrib = cassandraDescrib;
	}
	
	protected boolean isValueValueColumn(String columnName)
	{
		return columnName.equals(ElementBase.ValueField);
	}
	
	protected boolean isValueClassColumn(String columnName)
	{
		return columnName.equals(ElementBase.ValueClass);
	}	
	
	protected ConcurrentMap<String, String> serializeAppVariables(ElementBase elementBase, Map<String, Object> columnValueHash) throws Exception
	{
		ConcurrentMap<String, Object> serializeSource = null;
		if(columnValueHash != null)
		{
			serializeSource = (ConcurrentMap<String, Object>)columnValueHash.get("appVariables");
		}
		else
		{
			serializeSource = elementBase.appVariables;
		}
		String valuePrefix = ElementBase.AppVariableValuePrefix;
		String classPrefix = ElementBase.AppVariableClassPrefix;
		
		ConcurrentMap<String, String> stringMap = commonSerializeMap(serializeSource, valuePrefix,
				classPrefix);
		return stringMap;
	}	
	
	protected ConcurrentMap<String, String> serializeInNodes(ElementBase elementBase, Map<String, Object> columnValueHash) throws Exception
	{
		ConcurrentMap<String, Boolean> serializeSource = null;
		if(columnValueHash != null)
		{
			serializeSource = new ConcurrentHashMap<String, Boolean>();		
			serializeSource.put(columnValueHash.get("inNodesMap").toString(), true);
		}
		else
		{
			serializeSource = elementBase.getInNodesMap();
		}
		String valuePrefix = ElementBase.InNodesValuePrefix;
		String classPrefix = ElementBase.InNodesClassPrefix;		
		ConcurrentMap<String, String> stringMap = commonSerializeMap(serializeSource, valuePrefix,
				classPrefix);
		return stringMap;
	}
	
	protected ConcurrentMap<String, String> serializeOutNodes(ElementBase elementBase, Map<String, Object> columnValueHash) throws Exception
	{
		ConcurrentMap<String, Boolean> serializeSource = null;
		if(columnValueHash != null)
		{
			serializeSource = new ConcurrentHashMap<String, Boolean>();
			//new Object[]{ new Object[] { element.getId().toString(), true }}
			Object[][] outNodesIDValueArray = (Object[][])columnValueHash.get("outNodesMap");
			for(Object[] idValueArray : outNodesIDValueArray)
			{
				String id = (String)idValueArray[0];
				Boolean value = (Boolean)idValueArray[1];
				serializeSource.put(id, value);
			}
		}
		else
		{
			serializeSource = elementBase.getOutNodesMap(); 
		}
		String valuePrefix = ElementBase.OutNodesValuePrefix;
		String classPrefix = ElementBase.OutNodesClassPrefix;
		ConcurrentMap<String, String> stringMap = commonSerializeMap(serializeSource, valuePrefix,
				classPrefix);
		return stringMap;
	}	
	
	protected ConcurrentMap<String, String> serializeAttribute(ElementBase elementBase) throws Exception
	{
		ConcurrentMap<String, String> serializeSource = elementBase.getAttributeMap();
		String valuePrefix = ElementBase.AttributeValuePrefix;
		String classPrefix = ElementBase.AttributeClassPrefix;		
		ConcurrentMap<String, String> stringMap = commonSerializeMap(serializeSource, valuePrefix,
				classPrefix);
		return stringMap;
	}	

	protected <T> ConcurrentMap<String, String> commonSerializeMap(ConcurrentMap<String, T> serializeSource,
			String valuePrefix, String classPrefix) throws IOException,
			JsonGenerationException, JsonMappingException {
		ConcurrentMap<String, String> stringMap = null; 
		if(serializeSource != null && !serializeSource.isEmpty())
		{
			stringMap = new ConcurrentHashMap<String, String>();
			for(Map.Entry<String, T> entry : serializeSource.entrySet())
			{				
				stringMap.put(valuePrefix + entry.getKey(), new ObjectMapper().writeValueAsString(entry.getValue()));
				String typeName = null;
				if(entry.getValue() != null)
				{
					typeName = entry.getValue().getClass().getName();
				}
				stringMap.put(classPrefix + entry.getKey(), typeName);
			}
		}
		return stringMap;
	}
	
	protected ConcurrentMap<String, String> serialzeValue(ElementBase elementBase) throws Exception
	{
		ConcurrentMap<String, String> valueMap = null;	
		if(elementBase.value != null)
		{					
			valueMap = new ConcurrentHashMap<String, String>();
			ObjectMapper objectMapper = new ObjectMapper();
			String sValue = null;
			String sClass = null;
			try
			{
				sValue = objectMapper.writeValueAsString(elementBase.value);
				sClass = elementBase.value.getClass().getName();
				valueMap.put(ElementBase.ValueField, sValue);
				valueMap.put(ElementBase.ValueClass, sClass);
			}
			catch(Exception ex)
			{
				throw ex;
			}
		}
		return valueMap;
	}

	protected <T> void commonDeserializeMap(
			ConcurrentMap<String, T> deserializeDest,
			ConcurrentMap<String, String> deserializeSource, String classPrefix,
			String valuePrefix) throws ClassNotFoundException, IOException,
			JsonParseException, JsonMappingException {
		deserializeDest.clear();		
		if(deserializeSource == null || deserializeSource.isEmpty())
		{
			return;
		}
		ConcurrentMap<String, Class> VariableClassMap = new ConcurrentHashMap<String, Class>();
		for(Map.Entry<String, String> entry : deserializeSource.entrySet())
		{
			String columnName = entry.getKey(); 
			if(columnName.startsWith(classPrefix))
			{
				String variableName = columnName.substring(classPrefix.length());
				String className = entry.getValue();
				Class VariableClass = null;
				if(className != null && !className.equals(""))
				{
					VariableClass = Class.forName(className);
				}
				VariableClassMap.put(variableName, VariableClass);
			}
		}
		for(Map.Entry<String, String> entry : deserializeSource.entrySet())
		{
			String columnName = entry.getKey(); 
			if(columnName.startsWith(valuePrefix))
			{
				String variableName = columnName.substring(valuePrefix.length());
				String sVariableValue = entry.getValue();
				Class variableClass = (Class)VariableClassMap.get(variableName);
				T VariableValue = null;
				if(variableClass != null)
				{
					VariableValue = (T)new ObjectMapper().readValue(sVariableValue, variableClass);
				}
				deserializeDest.put(variableName, VariableValue);
			}
		}
	}

	protected void deserialzeValue(ElementBase elementBase, ConcurrentMap<String, String> valueMap) throws Exception
	{
		elementBase.value = null;
		if(valueMap == null || valueMap.isEmpty())
		{
			return;
		}
		String className = null;
		ConcurrentMap<String, Class> VariableClassMap = new ConcurrentHashMap<String, Class>();		
		for(Map.Entry<String, String> entry : valueMap.entrySet())
		{
			String columnName = entry.getKey(); 
			if(isValueClassColumn(columnName))
			{
				className = entry.getValue();
				break;
			}
		}
		
		for(Map.Entry<String, String> entry : valueMap.entrySet())
		{
			String columnName = entry.getKey(); 
			if(isValueValueColumn(columnName))
			{
				String sVariableValue = entry.getValue();
				Class variableClass = (Class)Class.forName(className);
				Object VariableValue = new ObjectMapper().readValue(sVariableValue, variableClass);
				elementBase.value = VariableValue;
				break;
			}
		}		
	}		
	
	protected void ConvertElementBaseValue(ElementBase elementBase, List<ColumnOrSuperColumn> columnList) throws Exception
	{
		ConcurrentMap<String, String> valueMap = new ConcurrentHashMap<String, String>();		
		for(ColumnOrSuperColumn column : columnList)
		{
			String columnName = new String(column.getColumn().getName());
			if(isValueClassColumn(columnName) || isValueValueColumn(columnName))
			{
				valueMap.put(columnName, new String(column.getColumn().getValue()));
			}				
		}
		deserialzeValue(elementBase, valueMap);
	}	
	
	protected <T> void commonConvertElementBaseMap(ElementBase elementBase, List<ColumnOrSuperColumn> columnList,
			String classPrefix, String valuePrefix,
			ConcurrentMap<String, T> deserializeDest) throws Exception
	{
		ConcurrentMap<String, String> deserializeSource = getDeserializeSource(columnList, classPrefix, valuePrefix);		
		commonDeserializeMap(deserializeDest, deserializeSource, classPrefix,
				valuePrefix);	
	}

	protected ConcurrentMap<String, String> getDeserializeSource(
			List<ColumnOrSuperColumn> columnList, String classPrefix,
			String valuePrefix) {
		ConcurrentMap<String, String> deserializeSource = new ConcurrentHashMap<String, String>();		
		for(ColumnOrSuperColumn column : columnList)
		{
			String columnName = new String(column.getColumn().getName());	
			if(columnName.startsWith(classPrefix) 
					|| columnName.startsWith(valuePrefix))
			{
				deserializeSource.put(columnName, new String(column.getColumn().getValue()));
			}
		}
		return deserializeSource;
	}
	
	protected List<ElementBase> get(List<String> keyList, IAction2<ElementBase, List<ColumnOrSuperColumn>> columnsConverter) throws Exception
	{
		List<ByteBuffer> keyBufferList = null;
		
		if(keyList != null)
		{
			keyBufferList = new ArrayList<ByteBuffer>();		
			for(String key : keyList)
			{		
				keyBufferList.add(ByteBuffer.wrap(key.getBytes()));
			}
		}
		
		List<ElementBase> resultList = CassandraHelper.batchGet(new IFunc<List<ColumnOrSuperColumn>, ElementBase>(){
				
				public ElementBase getValue(List<ColumnOrSuperColumn> param)
						throws Exception {				
					String className = null;
					for(ColumnOrSuperColumn column : param)
					{
						if(new String(column.getColumn().getName()).equals(ElementBase.ClassNameField))
						{
							className = new String(column.getColumn().getValue());
							break;
						}
					}
					return ElementBase.getElementInstance(className);
				}
			},
			cassandraDescrib,
			keyBufferList,
			columnsConverter);
		return resultList; 
	}
	
	protected ElementBase get(String key, IAction2<ElementBase, List<ColumnOrSuperColumn>> columnsConverter) throws Exception
	{
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		List<ElementBase> resultList = get(keyList, columnsConverter);		
		if(resultList == null || resultList.size() == 0)
		{
			return null;
		}
		else
		{
			return resultList.get(0);
		}
	}
	
	
	public void delete(String key) throws Exception
	{
		CassandraHelper.deleteRow(cassandraDescrib, key.getBytes());
	}
	
	protected IAction2<ElementBase, List<ColumnOrSuperColumn>> getColumnValuesAction()
	{
		return new IAction2<ElementBase, List<ColumnOrSuperColumn>>(){
			
			public void invoke(ElementBase tParam,
					List<ColumnOrSuperColumn> param2) throws Exception {
				ConvertElementBaseValue(tParam, param2);
				commonConvertElementBaseMap(tParam, param2, ElementBase.AppVariableClassPrefix,
						ElementBase.AppVariableValuePrefix, tParam.appVariables);
				
				commonConvertElementBaseMap(tParam, param2, ElementBase.InNodesClassPrefix,
						ElementBase.InNodesValuePrefix, tParam.getInNodesMap());
				commonConvertElementBaseMap(tParam, param2, ElementBase.OutNodesClassPrefix,
						ElementBase.OutNodesValuePrefix, tParam.getOutNodesMap());
				commonConvertElementBaseMap(tParam, param2, ElementBase.AttributeClassPrefix,
						ElementBase.AttributeValuePrefix, tParam.getAttributeMap());
			}			
		};
	}

		
	public ElementBase get(String key) throws Exception
	{
		return get(key, getColumnValuesAction());
	}
	
	public List<ElementBase> get(List<String> keyList) throws Exception
	{
		return get(keyList, getColumnValuesAction());
	}
	
	public List<ElementBase> getAll() throws Exception
	{
		List<String> keyList = null;
		return get(keyList);
	}

		
	public Object getValue(String key) throws Exception
	{
		return get(key, new IAction2<ElementBase, List<ColumnOrSuperColumn>>(){
			
			public void invoke(ElementBase tParam,
					List<ColumnOrSuperColumn> param2) throws Exception {
				ConvertElementBaseValue(tParam, param2);
			}			
		}).value;
	}
	
	protected class VariableColumnsConverter implements IFunc<ElementBase, List<Mutation>>
	{
		protected List<String> columnList = null;
		protected Map<String, Object> columnValueHash = null;
		public VariableColumnsConverter(List<String> columnList, Map<String, Object> columnValueHash)
		{
			this.columnList = columnList;
			this.columnValueHash = columnValueHash;
		}

		
		public List<Mutation> getValue(ElementBase elementBase) throws Exception {
			List<Mutation> resultList = new ArrayList();
			
			List<Mutation> valueMutation = null;
			if(this.columnList == null || this.columnList.contains("value"))
			{
				valueMutation = CassandraHelper.getDynamicMutationList(serialzeValue(elementBase));
			}
						
			List<Mutation> appVarMutationList = null;
			if(this.columnList == null || this.columnList.contains("appVariables"))
			{
				appVarMutationList = CassandraHelper.getDynamicMutationList(serializeAppVariables(elementBase, this.columnValueHash));
			}
			
			List<Mutation> outNodesMutationList = null;
			if(this.columnList == null || this.columnList.contains("outNodesMap"))
			{
				outNodesMutationList = CassandraHelper.getDynamicMutationList(serializeOutNodes(elementBase, this.columnValueHash));
			}
			
			List<Mutation> inNodesMutationList = null;
			if(this.columnList == null || this.columnList.contains("inNodesMap"))
			{
				inNodesMutationList = CassandraHelper.getDynamicMutationList(serializeInNodes(elementBase, this.columnValueHash));
			}
			
			List<Mutation> attributeMutationList = null;
			if(this.columnList == null || this.columnList.contains("attributeMap"))
			{
				attributeMutationList = CassandraHelper.getDynamicMutationList(serializeAttribute(elementBase));
			}
			 
			if(valueMutation != null)
			{
				resultList.addAll(valueMutation);
			}
			if(appVarMutationList != null)
			{
				resultList.addAll(appVarMutationList);
			}
			if(outNodesMutationList != null)
			{
				resultList.addAll(outNodesMutationList);
			}
			if(inNodesMutationList != null)
			{
				resultList.addAll(inNodesMutationList);
			}
			if(attributeMutationList != null)
			{
				resultList.addAll(attributeMutationList);
			}
			final long timestamp = System.currentTimeMillis();
			Mutation classNameMutation = CassandraHelper.getMutation(ElementBase.ClassNameField.getBytes(),
					elementBase.getClass().getName().getBytes(),
					timestamp);
			resultList.add(classNameMutation);
			return resultList;
		}		
	}

	
	public void add(final ElementBase elementBase) throws Exception
	{
		List<ElementBase> elementList = new ArrayList<ElementBase>();
		elementList.add(elementBase);
		CassandraHelper.batchUpdate(cassandraDescrib, CassandraHelper.getBatchPutColumnValuesMutation(elementList, null, new VariableColumnsConverter(null, null)), ConsistencyLevel.ONE);
	}

	
	public void update(final ElementBase elementBase, List<String> columnList, Map<String, Object> columnValueHash) throws Exception
	{
		IFunc<ElementBase, List<Mutation>> convertFunc = null;
		convertFunc = new VariableColumnsConverter(columnList, columnValueHash);	
		List<ElementBase> elementList = new ArrayList<ElementBase>();
		elementList.add(elementBase);
		CassandraHelper.batchUpdate(cassandraDescrib, CassandraHelper.getBatchPutColumnValuesMutation(elementList, columnList, convertFunc), ConsistencyLevel.ONE);
	}	
}
