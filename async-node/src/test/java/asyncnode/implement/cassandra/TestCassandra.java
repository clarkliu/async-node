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



import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.UUID;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.log4j.Logger;

import asyncnode.ElementStatusEnum;
import asyncnode.implement.cassandra.CassandraHelper.CassandraDescrib;
import asyncnode.implement.cassandra.TestCassandra.TestEntity.T3;
import asyncnode.core.IFunc;
import asyncnode.core.IAction2;


public class TestCassandra {
	public static Logger logger = Logger.getLogger(TestCassandra.class);
	
	//@Test
	public void testSerailize() throws Exception
	{
		UUID uuid1 = UUID.randomUUID();
		byte[] bUUID = FieldSerailzerBase.getStoreValue(UUID.class, uuid1);
		UUID uuid2 = (UUID)FieldSerailzerBase.getRuntimeValue(UUID.class, bUUID);
		
		ElementStatusEnum enum1 = ElementStatusEnum.NotifiedOutput;
		byte[] bEnum1 = FieldSerailzerBase.getStoreValue(ElementStatusEnum.class, enum1);
		ElementStatusEnum enum2 = (ElementStatusEnum)FieldSerailzerBase.getRuntimeValue(ElementStatusEnum.class, bEnum1);
		logger.info(enum1);
		logger.info(enum2);
	}
	
	//@Test
	private void testSerailizePrimitiveType() throws Exception
	{
		List<TestEntity> objList = new ArrayList<TestEntity>();
		TestEntity t1 = null;
		
		t1 = new TestEntity();
		t1.id = 1;
		objList.add(t1);
		
		t1 = new TestEntity();
		t1.bigDecimal = new BigDecimal(1.345);
		t1.booleanF = true;
		t1.BooleanF = false;
		t1.byteF = 12;
		t1.ByteF = 13;
		t1.charF = '4';
		t1.date = new Date();
		t1.doubleF = 14.61;
		t1.DoubleF = 15.72;
		t1.floatF = 12.83f;
		t1.FloatF = 13.94f;
		t1.id = 2;
		t1.IntegerF = 13;
		t1.intF = 14;
		t1.longF = 10;
		t1.LongF = 11l;
		t1.shortF = 126;
		t1.ShortF = 127;
		t1.string = "S129";
		t1.t3 = new TestEntity.T3();
		t1.t3.age = 35;
		t1.t3.Mark = "new Mark!";
		objList.add(t1);
		
		CassandraDescrib cassandraDescrib = new CassandraDescrib();
		cassandraDescrib.setColumnFamily("TestCF");
		cassandraDescrib.setConsistencyLevel(ConsistencyLevel.ONE);
		cassandraDescrib.setIp("127.0.0.1");
		cassandraDescrib.setPort(9160);
		cassandraDescrib.setKeySpace("AsyncNodeStore");
		CassandraHelper.batchUpdate(cassandraDescrib, CassandraHelper.getBatchPutColumnValuesMutation(objList, null, new IFunc<TestEntity, List<Mutation>>(){
			public List<Mutation> getValue(TestEntity param) throws Exception {
				
				List<TestEntity.T3> t3List = new ArrayList<TestEntity.T3>();
				t3List.add(param.t3);
				return CassandraHelper.getPutColumnRelationMutation(t3List, new IFunc<TestEntity.T3, String>(){
					public String getValue(T3 param) {
						
						return "T3_" + param.age;
					}					
				}, false);
			}
		}), ConsistencyLevel.ONE);
		List<TestEntity> testList2 = CassandraHelper.batchGet(TestEntity.class, cassandraDescrib, objList, new IAction2<TestEntity, List<ColumnOrSuperColumn>>(){
			public void invoke(TestEntity tParam, List<ColumnOrSuperColumn> columnList) throws Exception 
			{
				List<TestEntity.T3> t3List = CassandraHelper.getGetColumnRelationObjct(columnList, TestEntity.T3.class, new IFunc<String, Boolean>(){
					public Boolean getValue(String param) throws Exception {
						if(param != null && param.startsWith("T3_"))
						{
							return true;
						}
						else
						{
							return false;
						}
					}									
				}, new IAction2<TestEntity.T3, String>(){
					public void invoke(T3 tParam, String param2)
							throws Exception {
						tParam.age = Integer.parseInt(param2.substring("T3_".length()));
					}					
				});
				if(t3List != null && t3List.size() > 0)
				{
					tParam.t3 = t3List.get(0);
				}
			}
		});
		for(TestEntity t2 : testList2)
		{			
			logger.info("t2.bigDecimal is:" + t2.bigDecimal);
			logger.info("t2.booleanF is:" + t2.booleanF);
			logger.info("t2.BooleanF is:" + t2.BooleanF);
			logger.info("t2.byteF is:" + t2.byteF);
			logger.info("t2.ByteF is:" + t2.ByteF);
			logger.info("t2.charF is:" + t2.charF );
			logger.info("t2.date is:" + t2.date );
			logger.info("t2.doubleF  is:" + t2.doubleF);
			logger.info("t2.DoubleF is:" + t2.DoubleF);
			logger.info("t2.floatF is:" + t2.floatF);
			logger.info("t2.FloatF  is:" + t2.FloatF);
			logger.info("t2.id is:" + t2.id);
			logger.info("t2.IntegerF is:" + t2.IntegerF);
			logger.info("t2.intF is:" + t2.intF);
			logger.info("t2.longF is:" + t2.longF);
			logger.info("t2.LongF is:" + t2.LongF);
			logger.info("t2.shortF is:" + t2.shortF);
			logger.info("t2.ShortF is:" + t2.ShortF);
			logger.info("t2.string is:" + t2.string);
			if(t2.t3 != null)
			{
				logger.info("t2.t3 is:" + t2.t3.age);
				logger.info("t2.t3 Mark is:" + t2.t3.Mark);				
			}
			logger.info("\n\n\n");
		}
	}
	
	//@Test
	public void testJsonObject() throws JsonGenerationException, JsonMappingException, IOException
	{
		ConcurrentMap<String, Object> map1 = new ConcurrentHashMap<String, Object>();
		map1.put("1", 1);
		TestEntity testE1 = new TestEntity();
		testE1.intF = 2;
		testE1.floatF = 3.4f;
		map1.put("2", new ObjectMapper().writeValueAsString(testE1));
		
		String objValue = new ObjectMapper().writeValueAsString(map1);
		logger.info(objValue);
		
		ConcurrentMap<String, Object> testE2 = new ObjectMapper().readValue(objValue, ConcurrentMap.class);
		logger.info(testE2.get("1"));
		String str2 = ((String)testE2.get("2"));
		logger.info(str2);
		TestEntity testEntity = new ObjectMapper().readValue(str2, TestEntity.class);
		logger.info(testEntity.intF);
		logger.info(testEntity.floatF);
	}
	
	//@Test
	public void testJsonObject2() throws Exception
	{
		TestEntity testE1 = new TestEntity();
		testE1.intF = 2;
		testE1.floatF = 3.4f;
		testE1.t3 = new TestEntity.T3();
		testE1.t3.age = 10;
		String objValue = new ObjectMapper().writeValueAsString(testE1);
		logger.info(objValue);
		TestEntity testE2 = new ObjectMapper().readValue(objValue, TestEntity.class);
		
		logger.info(testE2.intF);
		logger.info(testE2.floatF);		
		logger.info(testE2.t3.age);
	}		
	
	@ICassandraKeyField(KeyFieldName = "id")
	static class TestEntity
	{
		@ICassandraField(ColumnName = "date", Serialer = FieldSerailzerBase.class)
		public Date date;
		
		@ICassandraField(ColumnName = "string", Serialer = FieldSerailzerBase.class)
		public String string;
		
		@ICassandraField(ColumnName = "byte", Serialer = FieldSerailzerBase.class)
		public byte byteF;
		
		@ICassandraField(ColumnName = "Byte", Serialer = FieldSerailzerBase.class)
		public Byte ByteF;		
		
		
		@ICassandraField(ColumnName = "short", Serialer = FieldSerailzerBase.class)
		public short shortF;
		
		@ICassandraField(ColumnName = "Short", Serialer = FieldSerailzerBase.class)
		public Short ShortF;	
		
		
		@ICassandraField(ColumnName = "int", Serialer = FieldSerailzerBase.class)
		public int intF;
		
		@ICassandraField(ColumnName = "Integer", Serialer = FieldSerailzerBase.class)
		public Integer IntegerF;			
		
		@ICassandraField(ColumnName = "long", Serialer = FieldSerailzerBase.class)
		public long longF;
		
		@ICassandraField(ColumnName = "Long", Serialer = FieldSerailzerBase.class)
		public Long LongF;	
		
		@ICassandraField(ColumnName = "float", Serialer = FieldSerailzerBase.class)
		public float floatF;
		
		@ICassandraField(ColumnName = "Float", Serialer = FieldSerailzerBase.class)
		public Float FloatF;	
		
		@ICassandraField(ColumnName = "double", Serialer = FieldSerailzerBase.class)
		public double doubleF;
		
		@ICassandraField(ColumnName = "Double", Serialer = FieldSerailzerBase.class)
		public Double DoubleF;	
		
		@ICassandraField(ColumnName = "BigDecimal", Serialer = FieldSerailzerBase.class)
		public BigDecimal bigDecimal;
		
		@ICassandraField(ColumnName = "char", Serialer = FieldSerailzerBase.class)
		public char charF;	
		
		
		@ICassandraField(ColumnName = "boolean", Serialer = FieldSerailzerBase.class)
		public boolean booleanF;
		
		@ICassandraField(ColumnName = "Boolean", Serialer = FieldSerailzerBase.class)
		public Boolean BooleanF;	
		
		
		@ICassandraField(ColumnName = "id", Serialer = FieldSerailzerBase.class)
		public int id;		
	
		public TestEntity()
		{
			
		}
		
		public T3 t3;
		public static class T3
		{
			public T3()
			{
				
			}
			
			public int age;
			
			public String Mark;
		}
	}	
}
