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

package asyncnode.implement.cassandra;

import java.lang.annotation.Retention;

import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Random;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import asyncnode.CommonUtil;
import asyncnode.GenericResult;
import asyncnode.ListHelper;
import asyncnode.core.IAction;
import asyncnode.core.IAction2;
import asyncnode.core.IFunc;
import org.codehaus.jackson.map.ObjectMapper;

public class CassandraHelper {	
	private static byte[] emptyByteArray = new byte[0];
	private static ByteBuffer emptyByteBuffer = ByteBuffer.wrap(emptyByteArray);
	public static final int DefaultGetSize = 100;
		
	public static Map<ByteBuffer, List<Mutation>> getMutationMap(String key, List<Mutation> mutationList)
	{
		Map<ByteBuffer, List<Mutation>> resultMap = new HashMap<ByteBuffer, List<Mutation>>();
		resultMap.put(ByteBuffer.wrap(key.getBytes()), mutationList);
		return resultMap;
	}
	
	public static Map<ByteBuffer, List<Mutation>> getMutationMap(Class objClass, Object keyObj, List<Mutation> mutationList) throws Exception
	{
		ByteBuffer key = ByteBuffer.wrap(keyObj.toString().getBytes());
		Map<ByteBuffer, List<Mutation>> valueMap = new HashMap<ByteBuffer, List<Mutation>>();
		valueMap.put(key, mutationList);
		return valueMap;
	}
	
	public static void setFieldValue(Object obj, Field field, Object value) throws IllegalArgumentException, IllegalAccessException
	{
		field.setAccessible(true);
		field.set(obj, value);
	}
	
	public static Object getFieldValue(Object obj, Field field) throws Exception
	{
		field.setAccessible(true);
		return field.get(obj);
	}
	
	public static Field[] getAncestorFields(Class classType)
	{
		List<Field> filedList = new ArrayList<Field>();
		while(classType != null)
		{
			filedList.addAll(Arrays.asList(classType.getDeclaredFields()));
			classType = classType.getSuperclass();
		}
		Field[] fieldArray = new Field[filedList.size()];
		for(Field field : filedList)
		{
			field.setAccessible(true);
		}
		return filedList.toArray(fieldArray);
	}
	
	public static <T> T getAncestorAnnotation(Class classType, Class<T> annotationType)
	{
		T tClass = (T)classType.getAnnotation(annotationType);
		if(tClass != null)
		{
			return tClass;
		}
		else
		{
			Class superClass = classType.getSuperclass();
			if(superClass != null)
			{
				return getAncestorAnnotation(superClass, annotationType);
			}
			else
			{
				return null;
			}
		}
	}
	
	public static List<ColumnOrSuperColumn> getPrefixColumns(final CassandraDescrib cassandraDescrib, 
			final String key,
			final String prefix, final int count) throws Exception
	{	
		final GenericResult<List<ColumnOrSuperColumn>> result = new GenericResult<List<ColumnOrSuperColumn>>();
		invokeCassandraCore(cassandraDescrib, new IAction<Client>(){
			public void invoke(Client client) throws Exception {
				
				ColumnParent columnParent = new ColumnParent();
				columnParent.column_family = cassandraDescrib.columnFamily;
				SlicePredicate slicePredict = new SlicePredicate();
				SliceRange sliceRange = new SliceRange();
				sliceRange.count = count;
				sliceRange.reversed = false;
				sliceRange.start = ByteBuffer.wrap((prefix + "_").getBytes());
				sliceRange.finish = ByteBuffer.wrap((prefix + "`").getBytes());
				slicePredict.slice_range = sliceRange; 								
				List<ColumnOrSuperColumn> resultList = client.get_slice(ByteBuffer.wrap(key.getBytes()), columnParent, slicePredict, cassandraDescrib.getConsistencyLevel());
				if(resultList == null || resultList.isEmpty())
				{
					result.setResult(null);
				}
				else
				{
					result.setResult(resultList);
				}
			}			
		});
		return result.getResult();
	}
	
	public static List<Mutation> getDynamicMutationList(Map<String, String> dynamicMap)
	{
		if(dynamicMap == null)
		{
			return null;
		}
		long timestamp = System.currentTimeMillis();
		List<Mutation> resultList = new ArrayList<Mutation>();
		for(Map.Entry<String, String> entry : dynamicMap.entrySet())
		{
			byte[] bColumnName = entry.getKey().getBytes();			
			byte[] bColumnValue = emptyByteArray;
			if(entry.getValue() != null)
			{
				bColumnValue = entry.getValue().getBytes();
			}
			Mutation mutation = getMutation(bColumnName, 
					bColumnValue, 
					timestamp);
			resultList.add(mutation);
		}
		return resultList;
	}
	
	public static void visitColumns(Class objClass, List<String> columnList, IAction2<Field, Field[]> visitAction) throws Exception
	{
		ICassandraKeyField keyCassandraField = getAncestorAnnotation(objClass, ICassandraKeyField.class);
		String keyFieldName = keyCassandraField.KeyFieldName();
		Field[] fieldArray = getAncestorFields(objClass);
		Field[] oldFieldArray = fieldArray;
		if(columnList != null)
		{					
			if(keyCassandraField == null)
			{
				throw new Exception("can't find key field define!");
			}
			final List<Field> fieldList = new ArrayList<Field>();
			
			for(final String columnName : columnList)
			{				
				ListHelper.first(Arrays.asList(fieldArray), new IFunc<Field, Boolean>(){
					public Boolean getValue(Field field) {
						if(field.getName().equals(columnName))
						{
							fieldList.add(field);
							return true;
						}
						else
						{
							return false;
						}
					}				
				});
			}
			fieldArray = new Field[fieldList.size()];
			fieldArray = fieldList.toArray(fieldArray);
		}
		Field keyField = null;
		for(Field field : oldFieldArray)
		{
			if(field.getName().equals(keyFieldName))
			{
				keyField = field; 
				break;
			}
		}		
		if(keyField == null)
		{
			throw new Exception("can't load key field!");
		}
		visitAction.invoke(keyField, fieldArray);
	}
	
	private static FieldSerailzerBase getFieldSerailzerBase(Field field, ICassandraField columnField) throws InstantiationException, IllegalAccessException
	{
		FieldSerailzerBase serializer = (FieldSerailzerBase)columnField.Serialer().newInstance();
		serializer.setFieldClass(field.getType());
		return serializer;
	}
	
	public static <T> Map<ByteBuffer,List<Mutation>> getBatchPutColumnValuesMutation(final List<T> objList, final List<String> columnList, IFunc<T, List<Mutation>> relationFunc) throws Exception
	{
		Map<ByteBuffer,List<Mutation>> mapList = new HashMap<ByteBuffer,List<Mutation>>();
		for(T obj : objList)
		{
			Map<ByteBuffer,List<Mutation>> mapItem = getPutColumnValuesMutation(obj, columnList);
			if(relationFunc != null)
			{
				List<Mutation> relationMutation = relationFunc.getValue(obj);
				if(mapItem != null && relationMutation != null)
				{
					for(List<Mutation> muteList : mapItem.values())
					{
						muteList.addAll(relationMutation);
						break;
					}
				}
			}
			mapList.putAll(mapItem);
		}
		return mapList;
	}
	
	public static void deleteRow(final CassandraDescrib cassandraDescrib, final byte[] row) throws Exception
	{
		CassandraHelper.invokeCassandraCore(cassandraDescrib, new IAction<Client>(){
			public void invoke(Client client) throws Exception {
				
				ColumnPath columnPath = new ColumnPath(cassandraDescrib.getColumnFamily());
				client.remove(ByteBuffer.wrap(row), columnPath, System.currentTimeMillis(), ConsistencyLevel.ONE);			
			}			
		});
	}
	
	private static void getAllMutation(final CassandraDescrib cassandraDescrib, final IAction<List<KeySlice>> keySliceAction) throws Exception
	{
		CassandraHelper.invokeCassandraCore(cassandraDescrib, new IAction<Client>(){
			public void invoke(Client client) throws Exception {
				
				KeyRange keyRange = new KeyRange();
				keyRange.start_key = emptyByteBuffer;
				keyRange.end_key = emptyByteBuffer;
				keyRange.setCount(Integer.MAX_VALUE);
				
				ColumnParent columnPath = new ColumnParent();
				columnPath.setColumn_family(cassandraDescrib.getColumnFamily());
				
				SlicePredicate predicate = new SlicePredicate();
				predicate.slice_range = new SliceRange();
				predicate.slice_range.start = emptyByteBuffer;
				predicate.slice_range.finish = emptyByteBuffer; 
				predicate.slice_range.count = Integer.MAX_VALUE;
				
				List<KeySlice> keySliceList = client.get_range_slices(columnPath, predicate, keyRange, ConsistencyLevel.ONE);
				keySliceAction.invoke(keySliceList);
			}			
		});
	}
	
	public static void getAllMutation(final CassandraDescrib cassandraDescrib, final IAction2<String, List<String>> action) throws Exception
	{	
		getAllMutation(cassandraDescrib, new IAction<List<KeySlice>>(){
			public void invoke(List<KeySlice> keySliceList) throws Exception {
				for(KeySlice keySlice : keySliceList)
				{
					if(keySlice.getColumns() == null || keySlice.getColumns().isEmpty())
					{
						continue;
					}
					
					String key = new String(keySlice.getKey());
					List<String> columnNameList = new ArrayList<String>();
					for(ColumnOrSuperColumn column : keySlice.getColumns())
					{
						columnNameList.add(new String(column.column.getName()));
					}
					action.invoke(key, columnNameList);
				}
			}});
	}
	
	public static Mutation getDeleteMutation(byte[] bColumnName)
	{
	    Mutation result = new Mutation();  
	    Deletion deletion = new Deletion();  
	    deletion.setTimestamp(System.currentTimeMillis());  
	    SlicePredicate predicate = new SlicePredicate();
	    predicate.column_names = new ArrayList<ByteBuffer>();
	    predicate.column_names.add(ByteBuffer.wrap(bColumnName));               
	    deletion.setPredicate(predicate);              
	    result.setDeletion(deletion);
	    return result;        
	}
	
	public static Mutation getMutation(byte[] bColumnName, 
			byte[] columnValue, 
			long timestamp)
	{
		Column column = new Column();
		column.setName(bColumnName);
		column.setValue(columnValue);
		column.setTimestamp(timestamp);
		ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
		columnOrSuperColumn.setColumn(column);
		Mutation columnMutation = new Mutation();
		columnMutation.setColumn_or_supercolumn(columnOrSuperColumn);
		return columnMutation;
	}
	
	public static <T> List<Mutation> getPutColumnRelationMutation(final List<T> objList, IFunc<T, String> nameFunc, boolean isSerialize) throws Exception
	{
		List<Mutation> mutationList = new ArrayList<Mutation>();
		final long timestamp = System.currentTimeMillis();
		for(T obj : objList)
		{
			if(obj == null)
			{
				continue;
			}
			String columnName = nameFunc.getValue(obj);
			byte[] bColumnName = columnName.getBytes();
			byte[] columnValue = FieldSerailzerBase.emptyByteArray;
			if(isSerialize)
			{
				ObjectMapper objectMapper = new ObjectMapper();
				columnValue = objectMapper.writeValueAsString(obj).getBytes();
			}
			Mutation columnMutation = getMutation(bColumnName, columnValue, timestamp);
			mutationList.add(columnMutation);
		}
		return mutationList;
	}

	public static <T> List<T> getGetColumnRelationObjct(final List<ColumnOrSuperColumn> columnList,
			Class<T> classType,
			IFunc<String, Boolean> isRelationFunc,
			IAction2<T, String> emptyAction) throws Exception
	{
		if(isRelationFunc == null)
		{
			return null;
		}
		List<T> resultList = new ArrayList<T>();
		for(ColumnOrSuperColumn column : columnList)
		{
			String columnName = new String(column.getColumn().getName());
			Boolean isOk = isRelationFunc.getValue(columnName);
			if(isOk)
			{
				String jsonValue = new String(column.getColumn().getValue());
				T itemValue = null;
				if(jsonValue != null && jsonValue.length() > 0)
				{
					ObjectMapper objectMapper = new ObjectMapper();				
					itemValue = objectMapper.readValue(jsonValue, classType);				
				}
				else
				{
					itemValue = classType.newInstance();
					emptyAction.invoke(itemValue, columnName); 
				}
				resultList.add(itemValue);
			}
		}
		return resultList;
	}
	
	public static Map<ByteBuffer,List<Mutation>> getPutColumnValuesMutation(final Object obj, List<String> columnList) throws Exception
	{
		final Map<ByteBuffer,List<Mutation>> result = new HashMap<ByteBuffer,List<Mutation>>();
		if(obj == null)
		{
			return result;
		}
		Class objClass = obj.getClass();		
		final long timestamp = System.currentTimeMillis();
		visitColumns(objClass, columnList, new IAction2<Field, Field[]>(){
			public void invoke(Field keyField, Field[] fieldArray) throws Exception {
				List<Mutation> mutationList = new ArrayList<Mutation>();
				for(Field field : fieldArray)
				{							
					ICassandraField columnField = (ICassandraField)field.getAnnotation(ICassandraField.class);
					if(columnField == null)
					{
						continue;
					}
					FieldSerailzerBase serializer = getFieldSerailzerBase(field, columnField);
					byte[] valueArray = serializer.getStoreValue(getFieldValue(obj, field));
					Column column = new Column();
					column.setName(columnField.ColumnName().getBytes());
					column.setValue(valueArray);
					column.setTimestamp(timestamp);
					ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
					columnOrSuperColumn.setColumn(column);
					Mutation columnMutation = new Mutation();
					columnMutation.setColumn_or_supercolumn(columnOrSuperColumn);
					mutationList.add(columnMutation);
				}
				
				ICassandraField keyColumnField = (ICassandraField)keyField.getAnnotation(ICassandraField.class);
				FieldSerailzerBase keySerializer = getFieldSerailzerBase(keyField, keyColumnField);
				byte[] keyValueArray = keySerializer.getStoreValue(getFieldValue(obj, keyField));	
				result.put(ByteBuffer.wrap(keyValueArray), mutationList);
			}			
		});
		return result;		
	}
	

	public static void invokeCassandraCore(CassandraDescrib cassandraDescrib, IAction<Cassandra.Client> action) throws Exception
	{

			TSocket socket = new TSocket(cassandraDescrib.getIp(), cassandraDescrib.getPort());
			String keyspace = cassandraDescrib.getKeySpace();
			String columnFamily = cassandraDescrib.getColumnFamily();
			ConsistencyLevel consistencyLevel = cassandraDescrib.getConsistencyLevel();			
			TTransport tr = new TFramedTransport(socket);
			try
			{
				TProtocol proto = new TBinaryProtocol(tr);				
				Cassandra.Client client = new Cassandra.Client(proto);
				tr.open();		 				
				client.set_keyspace(keyspace);
				if(action != null)
				{
					action.invoke(client);
				}
			}
			finally
			{
				if(tr != null)
				{
					tr.close();
				}
			}
	}

	private static Map<ByteBuffer, Map<String, List<Mutation>>> getColumnFamilyMap(final CassandraDescrib cassandraDescrib, final Map<ByteBuffer,List<Mutation>> valueMap)
	{
		Map<ByteBuffer, Map<String, List<Mutation>>> updateMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
		for(Map.Entry<ByteBuffer, List<Mutation>> entry : valueMap.entrySet())
		{
			List<Mutation> columnValueList = entry.getValue();
			Map<String, List<Mutation>> tableRowMap = new HashMap<String, List<Mutation>>();
			tableRowMap.put(cassandraDescrib.columnFamily, columnValueList);
			updateMap.put(entry.getKey(), tableRowMap);
		}
		return updateMap;
	}
	
	public static void batchUpdate(final CassandraDescrib cassandraDescrib, final Map<ByteBuffer,List<Mutation>> valueMap, 
			final ConsistencyLevel consistencyLevel) throws Exception
	{
		invokeCassandraCore(cassandraDescrib, new IAction<Cassandra.Client>(){
			public void invoke(Client client) throws Exception {
				Map<ByteBuffer, Map<String, List<Mutation>>> updateMap = getColumnFamilyMap(cassandraDescrib, valueMap);
				client.batch_mutate(updateMap, consistencyLevel);
			}
		});
	}
	
	public static void batchDelete(CassandraDescrib cassandraDescrib, List<String> keyList)
	{

	}
	
	private static class FieldSerializePair
	{
		public Field _field;
		public FieldSerailzerBase _serializer;
	}	
	
	private static Map<ByteBuffer, List<ColumnOrSuperColumn>> batchGet(CassandraDescrib cassandraDescrib, Client client, List<ByteBuffer> keyBufferList) throws Exception
	{
		ColumnParent columnParent = new ColumnParent();
		columnParent.column_family = cassandraDescrib.columnFamily;
		SlicePredicate slicePredict = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.count = Integer.MAX_VALUE;
		sliceRange.reversed = false;
		sliceRange.finish = emptyByteBuffer;
		sliceRange.start = emptyByteBuffer;
		slicePredict.slice_range = sliceRange; 						
		Map<ByteBuffer, List<ColumnOrSuperColumn>> resultList = null;
		if(keyBufferList != null)
		{
			resultList = client.multiget_slice(keyBufferList, columnParent, slicePredict, cassandraDescrib.getConsistencyLevel());
		}
		else
		{
			final GenericResult<Map<ByteBuffer, List<ColumnOrSuperColumn>>> genericResult = new GenericResult<Map<ByteBuffer, List<ColumnOrSuperColumn>>>();
			getAllMutation(cassandraDescrib, new IAction<List<KeySlice>>(){
				public void invoke(List<KeySlice> keySliceList) throws Exception {
					Map<ByteBuffer, List<ColumnOrSuperColumn>> allRows = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
					for(KeySlice keySlice : keySliceList)
					{				
						allRows.put(ByteBuffer.wrap(keySlice.getKey()), keySlice.getColumns());
					}
					genericResult.setResult(allRows);					
				}});			
			resultList = genericResult.getResult();
		}
		Map<ByteBuffer, List<ColumnOrSuperColumn>> filterList = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
		for(Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : resultList.entrySet())
		{
			if(entry.getValue() == null || entry.getValue().isEmpty())
			{
				continue;
			}
			filterList.put(entry.getKey(), entry.getValue());
		}
		return filterList;
	}
	
	public static Map<ByteBuffer, List<ColumnOrSuperColumn>> singleGet(final CassandraDescrib cassandraDescrib, String key) throws Exception
	{
		List<ByteBuffer> byteList = new ArrayList<ByteBuffer>();
		byteList.add(ByteBuffer.wrap(key.getBytes()));
		return batchGet(cassandraDescrib, byteList);
	}
	
	public static Map<ByteBuffer, List<ColumnOrSuperColumn>> batchGet(final CassandraDescrib cassandraDescrib, final List<ByteBuffer> keyBufferList) throws Exception
	{
		final GenericResult<Map<ByteBuffer, List<ColumnOrSuperColumn>>> result = new GenericResult<Map<ByteBuffer, List<ColumnOrSuperColumn>>>();
		invokeCassandraCore(cassandraDescrib, new IAction<Cassandra.Client>(){
			public void invoke(Client client) throws Exception {
				
				result.setResult(batchGet(cassandraDescrib, client, keyBufferList));
			}});
		return result.getResult();
	}
	
	public static class ClassFieldsVisitor<T> implements IFunc<List<ColumnOrSuperColumn>, Map<String, FieldSerializePair>>
	{
		private IFunc<List<ColumnOrSuperColumn>, T> getInstanceFunc = null;
		public ClassFieldsVisitor(IFunc<List<ColumnOrSuperColumn>, T> getInstanceFunc)
		{
			this.getInstanceFunc = getInstanceFunc;
		}

		public Map<String, FieldSerializePair> getValue(
				List<ColumnOrSuperColumn> param) throws Exception {
			Class classType = getInstanceFunc.getValue(param).getClass();						
			final GenericResult<Map<String, FieldSerializePair>> resultMap = new GenericResult<Map<String, FieldSerializePair>>();
			visitColumns(classType, null, new IAction2<Field, Field[]>(){
				public void invoke(final Field keyField, final Field[] fieldArray) throws Exception {
					final Map<String, FieldSerializePair> columnSerializerMap = getColumnSerializerMap(fieldArray);
					resultMap.setResult(columnSerializerMap);
				}			
			});
			return resultMap.getResult();
		}
	}	

	public static <T> List<T> batchGet(final IFunc<List<ColumnOrSuperColumn>, T> getInstanceFunc, 
			final CassandraDescrib cassandraDescrib, 
			final List<ByteBuffer> keyBufferList,
			final IAction2<T, List<ColumnOrSuperColumn>> relationFunc) throws Exception
	{				
		final GenericResult<List<T>>	resultList = new GenericResult<List<T>>(); 
		invokeCassandraCore(cassandraDescrib, new IAction<Cassandra.Client>(){
			public void invoke(Client client) throws Exception {
				ClassFieldsVisitor classFieldsVisitor = new ClassFieldsVisitor(getInstanceFunc);
				Map<ByteBuffer, List<ColumnOrSuperColumn>> queryResultList = batchGet(cassandraDescrib, client, keyBufferList);
				List<T> batchResultList = batchGet(cassandraDescrib, client,
						classFieldsVisitor,
						keyBufferList,
						getInstanceFunc,
						relationFunc,
						queryResultList);		
				resultList.setResult(batchResultList);
			}	
		});
		return resultList.getResult();
	}
	
	private static Map<String, FieldSerializePair> getColumnSerializerMap(Field[] fieldArray) throws Exception
	{
		final Map<String, FieldSerializePair> columnSerializerMap = new HashMap<String, FieldSerializePair>();
		for(Field field : fieldArray)
		{			
			ICassandraField columnField = (ICassandraField)field.getAnnotation(ICassandraField.class);
			if(columnField == null)
			{
				continue;
			}
			FieldSerailzerBase columnSerializer = getFieldSerailzerBase(field, columnField);
			FieldSerializePair pair = new FieldSerializePair();
			pair._field = field;
			pair._serializer = columnSerializer;
			columnSerializerMap.put(columnField.ColumnName(), pair);
		}		
		return columnSerializerMap;
	}
		
	public static <T> List<T> batchGet(final Class<T> classType, final CassandraDescrib cassandraDescrib, List<T> keyList,
			final IAction2<T, List<ColumnOrSuperColumn>> relationFunc) throws Exception
	{		
		return CommonUtil.getPagedList(keyList, DefaultGetSize, new IFunc<List<T>, List<T>>(){
			public List<T> getValue(List<T> keyList) throws Exception {
				
				return batchGetOnePage(classType, cassandraDescrib, keyList,
						relationFunc);
			}});
	}
	
	public static <T> List<T> batchGetOnePage(final Class<T> classType, final CassandraDescrib cassandraDescrib, final List<T> keyList,
			final IAction2<T, List<ColumnOrSuperColumn>> relationFunc) throws Exception
	{				
		final GenericResult<List<T>>	resultList = new GenericResult<List<T>>(); 
		visitColumns(classType, null, new IAction2<Field, Field[]>(){
			public void invoke(final Field keyField, final Field[] fieldArray) throws Exception {
				invokeCassandraCore(cassandraDescrib, new IAction<Cassandra.Client>(){
					public void invoke(Client client) throws Exception {	
						final ICassandraField keyColumnField = (ICassandraField)keyField.getAnnotation(ICassandraField.class);
						final FieldSerailzerBase keySerializer = getFieldSerailzerBase(keyField, keyColumnField);
						List<ByteBuffer> keyBufferList = null;
						
						if(keyList != null)
						{
							keyBufferList = new ArrayList<ByteBuffer>();
							for(Object keyObj : keyList)
							{														
								ByteBuffer keyBuffer = ByteBuffer.wrap(keySerializer.getStoreValue(getFieldValue(keyObj, keyField)));
								keyBufferList.add(keyBuffer);
							}
						}

						final Map<String, FieldSerializePair> columnSerializerMap = getColumnSerializerMap(fieldArray);						
						Map<ByteBuffer, List<ColumnOrSuperColumn>> queryResultList = batchGet(cassandraDescrib, client, keyBufferList);
						List<T> batchResultList = batchGet(cassandraDescrib, client,
								new IFunc<List<ColumnOrSuperColumn>, Map<String, FieldSerializePair>>(){
									public Map<String, FieldSerializePair> getValue(
											List<ColumnOrSuperColumn> param) throws Exception {
										return columnSerializerMap;
									}},
								keyBufferList,
								new IFunc<List<ColumnOrSuperColumn>, T>(){
									public T getValue(List<ColumnOrSuperColumn> param)
											throws Exception {
										return classType.newInstance();
									}},
								relationFunc,
								queryResultList);							
						resultList.setResult(batchResultList);
					}	
				});
			}			
		});		
		return resultList.getResult();
	}
	
	private static <T> List<T> batchGet(final CassandraDescrib cassandraDescrib, Client client,
			IFunc<List<ColumnOrSuperColumn>, Map<String, FieldSerializePair>> fieldSerilizeFunc,
			final List<ByteBuffer> keyBufferList,
			IFunc<List<ColumnOrSuperColumn>, T> newFunc,
			final IAction2<T, List<ColumnOrSuperColumn>> relationFunc,
			Map<ByteBuffer, List<ColumnOrSuperColumn>> resultList) throws Exception
	{		
		final List<T> returnList = new ArrayList<T>();
		for(Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> resultEntity : resultList.entrySet())
		{
			Map<String, FieldSerializePair> columnSerializerMap = fieldSerilizeFunc.getValue(resultEntity.getValue());
			T returnEntity = newFunc.getValue(resultEntity.getValue());
			for(ColumnOrSuperColumn column : resultEntity.getValue())
			{
				String columnName = new String(column.getColumn().getName());
				FieldSerializePair pair = columnSerializerMap.get(columnName);
				if(pair == null)
				{
					continue;
				}
				FieldSerailzerBase fieldSerailzerBase  = pair._serializer;
				Object objValue = fieldSerailzerBase.getRuntimeValue(column.getColumn().getValue());
				setFieldValue(returnEntity, pair._field, objValue);
			}
			
			if(relationFunc != null && returnEntity != null)
			{
				relationFunc.invoke(returnEntity, resultEntity.getValue());
			}
			returnList.add(returnEntity);
		}		
		return returnList;
	}
	
	public static class CassandraDescrib {
		private String ip;
		private int port;		
		private String keySpace;
		private String columnFamily;
		private ConsistencyLevel consistencyLevel;
		
		public CassandraDescrib()
		{
			this.consistencyLevel = ConsistencyLevel.ONE;
			ColumnOrSuperColumn sc = null;
		}

		public String getIp() {
			return ip;
		}
		public void setIp(String ip) {
			this.ip = ip;
		}
		public int getPort() {
			return port;
		}
		public void setPort(int port) {
			this.port = port;
		}
		public String getKeySpace() {
			return keySpace;
		}
		public void setKeySpace(String keySpace) {
			this.keySpace = keySpace;
		}
		public String getColumnFamily() {
			return columnFamily;
		}
		public void setColumnFamily(String columnFamily) {
			this.columnFamily = columnFamily;
		}
		public ConsistencyLevel getConsistencyLevel() {
			return consistencyLevel;
		}
		public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
			this.consistencyLevel = consistencyLevel;
		}
	}	
}