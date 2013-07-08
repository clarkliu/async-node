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

package asyncnode;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import asyncnode.App;
import asyncnode.AsyncHelper;
import asyncnode.AsyncNodeRunner;
import asyncnode.CommonUtil;
import asyncnode.ElementBase;
import asyncnode.ElementBuilder;
import asyncnode.IAsyncNodeSchdule;

import com.google.inject.Singleton;

import asyncnode.App.AppContext;
import asyncnode.implement.CassandraAsyncNodeDao;
import asyncnode.implement.CassandraAsyncQueueDao;
import asyncnode.implement.CassandraWorkFlowDao;
import asyncnode.implement.ElementProvider;
import asyncnode.implement.MemoryAsyncNodeDao;
import asyncnode.implement.MemoryAsyncQueueDao;
import asyncnode.implement.StoreableElementDao;
import asyncnode.implement.ThreadProxy;
import asyncnode.implement.cassandra.CassandraHelper.CassandraDescrib;
import asyncnode.rpc.ClientResultContainer;
import asyncnode.TestAsyncNodeClientService.TestProcessServiceElement;
import asyncnode.core.IAction;
import asyncnode.core.IFunc;

public class TestAsyncElement {	
	public static Logger logger = Logger.getLogger(TestAsyncElement.class);
	
	public void dumpNodeList(String name, ConcurrentMap<String, ElementBase> elementBaseList)
	{
		StringBuilder sbOutput = new StringBuilder();
		sbOutput.append("dumpNodeList " + name + "\n");
		for(Entry<String, ElementBase> entry : elementBaseList.entrySet())
		{
			sbOutput.append(entry.getValue().getClassName() + "," + entry.getValue().getId().toString() + ":" + entry.getValue().getStatus() + "\n");
		}
		logger.info(sbOutput.toString());
	}
	
	public void dumpQueue(String name, ConcurrentMap<Integer, Set<String[]>> queue)
	{
		StringBuilder sbOutput = new StringBuilder();	
		sbOutput.append("dumpQueue " + name + "\n");
		for(Entry<Integer, Set<String[]>> entry : queue.entrySet())
		{			
			sbOutput.append("Queuey key:" + String.valueOf(entry.getKey()) + "  --> :" + "\n");
			for(String[] v : entry.getValue())
			{
				sbOutput.append(v[0] + "," + v[1] + ",");
			}
		}
		logger.info(sbOutput.toString());
	}
	
	private static UUID buildAsyncTree() throws Exception
	{
		UUID workflowID = UUID.randomUUID();		
		ElementBuilder elementBuilder = new ElementBuilder(workflowID);
		elementBuilder.add("EA", new EA())
			.add("EB", new EB())
			.add("EC", new EC())
			.add("ED", new ED())
			.add("EE", new EE())
			.add("EBC", new EBC())
			.add("EDE", new EDE())
			.add("EResult", new EResult())
				.addMap("EBC", EBC.AttrB, "EB")
				.addMap("EBC", EBC.AttrC, "EC")
				.addMap("EDE", EDE.AttrD, "ED")
				.addMap("EDE", EDE.AttrE, "EE")
				.addMap("EResult", EResult.AttrA, "EA")
				.addMap("EResult", EResult.AttrBC, "EBC")
				.addMap("EResult", EResult.AttrDE, "EDE")
				.build();
		return workflowID;
	}
	

		
	private static class TestAppModule extends App.DefaultAppModule
	{
		@Override
		protected void configure() {
			AppContext appContext = new AppContext();
			appContext.setMaxThreadCount(100);
			appContext.setSleepInterval(0);
			appContext.setDefaultSchduleCycle(10);
			this.bind(AppContext.class).toInstance(appContext);
			
			CassandraDescrib cassandraDescrib = null;
			
			cassandraDescrib = new CassandraDescrib();
			cassandraDescrib.setKeySpace("AsyncNodeStore");
			cassandraDescrib.setColumnFamily("AsyncNode");
			cassandraDescrib.setConsistencyLevel(ConsistencyLevel.ONE);
			cassandraDescrib.setIp("127.0.0.1");
			cassandraDescrib.setPort(9160);	
			CassandraAsyncNodeDao CassandraAsyncNodeDao = new CassandraAsyncNodeDao();
			CassandraAsyncNodeDao.setCassandraDescrib(cassandraDescrib);
			this.bind(CassandraAsyncNodeDao.class).toInstance(CassandraAsyncNodeDao);
			
			
			cassandraDescrib = new CassandraDescrib();
			cassandraDescrib.setKeySpace("AsyncNodeStore");
			cassandraDescrib.setColumnFamily("AsyncQueue");
			cassandraDescrib.setConsistencyLevel(ConsistencyLevel.ONE);
			cassandraDescrib.setIp("127.0.0.1");
			cassandraDescrib.setPort(9160);
			CassandraAsyncQueueDao cassandraAsyncQueueDao = new CassandraAsyncQueueDao();
			cassandraAsyncQueueDao.setCassandraDescrib(cassandraDescrib);
			this.bind(CassandraAsyncQueueDao.class).toInstance(cassandraAsyncQueueDao);
			
			
			cassandraDescrib = new CassandraDescrib();
			cassandraDescrib.setKeySpace("AsyncNodeStore");
			cassandraDescrib.setColumnFamily("AsyncWorkFlow");
			cassandraDescrib.setConsistencyLevel(ConsistencyLevel.ONE);
			cassandraDescrib.setIp("127.0.0.1");
			cassandraDescrib.setPort(9160);	
			CassandraWorkFlowDao cassandraWorkFlowDao = new CassandraWorkFlowDao();
			cassandraWorkFlowDao.setCassandraDescrib(cassandraDescrib);
			this.bind(CassandraWorkFlowDao.class).toInstance(cassandraWorkFlowDao);		
			
			this.bind(MemoryAsyncNodeDao.class).toInstance(new MemoryAsyncNodeDao());
			this.bind(MemoryAsyncQueueDao.class).toInstance(new MemoryAsyncQueueDao());
			this.bind(ElementProvider.class).toInstance(new ElementProvider());
			this.bind(StoreableElementDao.class).toInstance(new TestStoreableElementDao());
			
			this.bind(Logger.class).toInstance(Logger.getLogger(App.class));
			this.bind(ExceptionHandler.class).toInstance(new ExceptionHandler());
		}
	}
	
	@Singleton
	private static class TestStoreableElementDao extends StoreableElementDao
	{
		@Override
		public boolean runByPriority() throws Exception
		{
			logger.info("-----------------initAllStorage");
			
			memoryAsyncQueueDao.moveBufferQueue();
			memoryAsyncNodeDao.moveBufferElement();
			
			initAllStorage();
			
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
	}
	

	
	//@Before
	public  void init() throws Exception
	{
		TestElementBase.isPersistent = true;
	}
	
	//@Test
	public void testVariable() throws Exception
	{
		App.initConfig();
		UUID workflowID = UUID.randomUUID();		
		ElementBuilder elementBuilder = new ElementBuilder(workflowID);
		elementBuilder.add("child", new EVariable1(2))
		.add("parent", new EVariable1(3))			
			.addMap("parent", "Refer", "child")				
			.build();		
		this.runWorkFlow();
	}
	
	//@Test
	public  void testTwoNode() throws Exception
	{
		App.initConfig();
		UUID workflowID = UUID.randomUUID();		
		ElementBuilder elementBuilder = new ElementBuilder(workflowID);
		elementBuilder.add("E1", new E1())
			.add("E2", new E2())			
				.addMap("E2", E2.Attr1, "E1")				
				.build();
		int sleepInterval = App.getConfig().getInstance(App.AppContext.class).getSleepInterval();
		while(true)
		{
			if(AsyncNodeRunner.runByPriority())
			{
				if(sleepInterval > 0)
				{
					Thread.sleep(sleepInterval);
				}
			}
			else
			{
				Thread.sleep(1000);
			}
		}
	}
	
	//@Test
	public  void testTwoNodeCassandra() throws Exception
	{
		App.initConfig(new TestAppModule(), null);
		UUID workflowID = UUID.randomUUID();		
		ElementBuilder elementBuilder = new ElementBuilder(workflowID);
		elementBuilder.add("E1", new E1())
			.add("E2", new E2())			
				.addMap("E2", E2.Attr1, "E1")				
				.build();
		int sleepInterval = App.getConfig().getInstance(App.AppContext.class).getSleepInterval();
		while(true)
		{
			if(AsyncNodeRunner.runByPriority())
			{
				if(sleepInterval > 0)
				{
					Thread.sleep(sleepInterval);
				}
			}
			else
			{
				Thread.sleep(1000);
			}
		}
	}
	
	//@Test
	public  void testInMemoryTree() throws Exception
	{
		TestElementBase.isPersistent = false;
		testRunNodeTree();
	}
	
	//@Test
	public void testRunNodeTree() throws Exception
	{
		App.initConfig();
		UUID workflowID = buildAsyncTree();
		runWorkFlow();
	}
	
	//@Test
	public void testRunNodeTreeFromCassandra() throws Exception
	{
		App.initConfig(new TestAppModule(), null);
		UUID workflowID = buildAsyncTree();
		runWorkFlow();
	}
	
	//@Test
	public void testRunDynamic() throws Exception
	{
		App.initConfig();
		
		UUID uuid = UUID.randomUUID();
		ElementBuilder elementBuilder = new ElementBuilder(uuid);
		elementBuilder.add("Dyn1", new Dyn1());
		elementBuilder.build();
		
		AsyncNodeRunner.run(null, new IAction(){
			public void invoke(Object tParam) throws Exception {
				Thread.sleep(100);
			}
		});
		
		while(true)
		{
			Thread.sleep(1000);
		}
	}
	
	//@Test
	public void testRunDynamicFromCassandra() throws Exception
	{
		App.initConfig(new TestAppModule(), null);
		
		UUID uuid = UUID.randomUUID();
		ElementBuilder elementBuilder = new ElementBuilder(uuid);
		elementBuilder.add("Dyn1", new Dyn1());
		elementBuilder.build();
		
		AsyncNodeRunner.run(null, new IAction(){
			public void invoke(Object tParam) throws Exception {
				Thread.sleep(100);
			}
		});
		
		while(true)
		{
			Thread.sleep(1000);
		}		
	}
	
	//@Test
	public void testTreeDynResult() throws Exception
	{
		App.initConfig();
		UUID uuid = UUID.randomUUID();
		ElementBuilder elementBuilder = new ElementBuilder(uuid);
		elementBuilder.add("EA", new EA())
			.add("EB", new EB())
			.add("EC", new EC())
			.add("ED", new ED())
			.add("EE", new EE())
			.add("EBC", new EBC())
			.add("EDE", new EDE())
			.add("EResult", new EResult())
				.addMap("EBC", EBC.AttrB, "EB")
				.addMap("EBC", EBC.AttrC, "EC")
				.addMap("EDE", EDE.AttrD, "ED")
				.addMap("EDE", EDE.AttrE, "EE")
				.addMap("EResult", EResult.AttrA, "EA")
				.addMap("EResult", EResult.AttrBC, "EBC")
				.addMap("EResult", EResult.AttrDE, "EDE")
				.build();
		AsyncNodeRunner.run(null, new IAction(){
			public void invoke(Object tParam) throws Exception {
				
				Thread.sleep(100);
			}
		});		
		
		while(true)
		{
			Thread.sleep(1000);
		}
	}
	
	//@Test
	public void testTreeDynResultFromCassandra() throws Exception
	{
		App.initConfig(new TestAppModule(), null);
		UUID uuid = UUID.randomUUID();
		ElementBuilder elementBuilder = new ElementBuilder(uuid);
		elementBuilder.add("EA", new EA())
			.add("EB", new EB())
			.add("EC", new EC())
			.add("ED", new ED())
			.add("EE", new EE())
			.add("EBC", new EBC())
			.add("EDE", new EDE())
			.add("EResult", new EResult())
				.addMap("EBC", EBC.AttrB, "EB")
				.addMap("EBC", EBC.AttrC, "EC")
				.addMap("EDE", EDE.AttrD, "ED")
				.addMap("EDE", EDE.AttrE, "EE")
				.addMap("EResult", EResult.AttrA, "EA")
				.addMap("EResult", EResult.AttrBC, "EBC")
				.addMap("EResult", EResult.AttrDE, "EDE")
				.build();
		AsyncNodeRunner.run(null, new IAction(){
			public void invoke(Object tParam) throws Exception {
				
				Thread.sleep(100);
			}
		});	
		
		while(true)
		{
			Thread.sleep(1000);
		}
	}	
	
	//@Test
	public void testConcurrentMap() throws InterruptedException
	{
		final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
		for(int i1 = 0; i1 < 10; i1++)
		{
			map.put(String.valueOf(i1), String.valueOf(i1 * 10));
		}
		
		Thread th = new Thread(new Runnable(){
			public void run() {
				for(int i1 = 0; i1 < 10; i1++)
				{
					logger.info("in sub thread");
					map.put(String.valueOf(i1 + 100), String.valueOf(i1 * 10));
					
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}});
		th.start();
		for(Entry<String, String> entry : map.entrySet())
		{
			logger.info("in main thread");
			Thread.sleep(1000);
		}
	}
	
	//@Test
	public void testRunClientService() throws Exception
	{
		App.initConfig();
		ConcurrentHashMap<String, Class> serviceHashMap = new ConcurrentHashMap<String, Class>();
		serviceHashMap.put("www.Testasyncnode.com", TestProcessServiceElement.class);
		ElementBuilder.registerService(serviceHashMap);
		ElementBuilder.buildClient("www.Testasyncnode.com", 10, new ElementBase(){
			@Override
			public void executeCore() throws Exception {
				
				logger.info("............client call back output:" + ((ClientResultContainer)this.getAttribute("Result")).getRequestResult().getValue());
				this.notifyFinish(null, null);
			}		
		});
		runWorkFlow();
	}	
	
	//@Test
	public void testRunOutputElements() throws Exception
	{
		App.initConfig();
		ElementBuilder eb = new ElementBuilder(UUID.randomUUID());
		eb.add("out13", new Out13())
			.add("out1", new Out1())
			.add("out3", new Out3())
			.addMap("out1", "child", "out13")
			.addMap("out3", "child", "out13")
			.build();
		runWorkFlow();
	}
	
	//@Test
	public  void testReflection() throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException, SecurityException, NoSuchMethodException
	{
		Constructor<EA> ea = EA.class.getConstructor(null);
		ea.setAccessible(true);
		EA ea2 = ea.newInstance();
		logger.info(ea2.getClass());
	}


	static int iInterval = 0;
	static int addCount = 0;
	public void runWorkFlow() throws Exception
	{
		AsyncNodeRunner.run(new IAction(){
			public void invoke(Object tParam) throws Exception {
				dumpNodeList("getElementBaseMap", App.getConfig().getInstance(MemoryAsyncNodeDao.class).getElementBaseMap());
				dumpNodeList("getPendingElementBaseMap", App.getConfig().getInstance(MemoryAsyncNodeDao.class).getPendingElementBaseMap());
				dumpQueue("getAsyncQueue", App.getConfig().getInstance(MemoryAsyncQueueDao.class).getAsyncQueue());
				dumpQueue("getPendingAsyncQueue", App.getConfig().getInstance(MemoryAsyncQueueDao.class).getPendingAsyncQueue());				
				Thread.sleep(1000);
			}
		}, new IAction(){
			public void invoke(Object tParam) throws Exception {
				//Thread.sleep(10);
				
				AsyncHelper.asyncInvoke(new IAction(){
					public void invoke(Object tParam) throws Exception {
						int tValue = iInterval++;
						if(tValue % 3 == 0 && tValue < 20)
						{
							addCount++;
							App.getConfig().getInstance(Logger.class).info(".......create tree!");
							buildAsyncTree();
							Thread.sleep(1000);
						}	
					}});				
			}
		});
		while(true)
		{
			Thread.sleep(1000);
		}
	}
}

class Dyn1 extends TestElementBase
{
	public Dyn1()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		TestAsyncElement.logger.info("Dyn1 1!");		
		UUID uuid = this.getWorkFlowID();
		ElementBuilder elementBuilder = new ElementBuilder(uuid);
		elementBuilder.add("Dyn2", new Dyn2());
		elementBuilder.add("Dyn3", new Dyn3());
		elementBuilder.add("DynResult", new DynResult());
		elementBuilder.addMap("DynResult", "v2", "Dyn2");
		elementBuilder.addMap("DynResult", "v3", "Dyn3");		
		this.notifyFinish(2, null, elementBuilder);		
	}
}

class Dyn2 extends TestElementBase
{
	public Dyn2()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		TestAsyncElement.logger.info("Dyn2 2!");
		this.notifyFinish((Integer)this.getAttribute(ElementBase.DynamicAttributeName) + 3, null);		
	}
}

class Dyn3 extends TestElementBase
{
	public Dyn3()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		TestAsyncElement.logger.info("Dyn3 3!");
		this.notifyFinish((Integer)this.getAttribute(ElementBase.DynamicAttributeName) + 4, null);	
	}
}

class DynResult extends TestElementBase
{
	public DynResult()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		TestAsyncElement.logger.info("out 13!");
		Integer result = (Integer)this.getAttribute("v2") * (Integer)this.getAttribute("v3");
		this.notifyFinish(result, appVariables);
		TestAsyncElement.logger.info("................result is:" + result);
	}
}




class Out13 extends TestElementBase
{
	public Out13()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		TestAsyncElement.logger.info("out 13!");
		this.notifyFinish(1, null);		
	}
}

class Out1 extends TestElementBase
{
	public Out1()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		TestAsyncElement.logger.info("out 1!");
		this.notifyFinish(1, null);		
	}
}

class Out3 extends TestElementBase
{
	public Out3()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		TestAsyncElement.logger.info("out 3!");
		this.notifyFinish(1, null);		
	}
}

class EVariable1 extends TestElementBase
{
	public EVariable1()
	{
		
	}
	
	public EVariable1(Integer v1) throws Exception
	{
		this.setVariable("Value", v1);
	}
	
	@Override
	public void executeCore() throws Exception {
		Integer v1 = (Integer)this.getVariable("Value");
		Integer referValue = (Integer)this.getAttribute("Refer");
		if(referValue == null)
		{
			referValue = 1;
		}
		
		Integer result = (v1 + 1) * referValue;
		App.getConfig().getInstance(Logger.class).info("notify result is:" + result);
		this.notifyFinish(result, null);		
	}
}

class E1 extends TestElementBase
{
	public E1()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		Thread.sleep(1000);
		this.notifyFinish(1, null);		
	}
}

class E2 extends TestElementBase
{
	public E2()
	{}
	
	public static final String Attr1 = "E1";

	
	public int get1() throws Exception
	{
		return ((Integer)this.getAttribute(Attr1)).intValue();
	}
	
	@Override
	public void executeCore() throws Exception {
		App.getConfig().getInstance(Logger.class).info("this.get1() + 2 result:" + (this.get1() + 2));
		this.notifyFinish(this.get1() + 2, null);
	}
}


class EA extends TestElementBase
{
	public EA()
	{
		
	}
	
	@Override
	public void executeCore() throws Exception {
		
		Thread.sleep(1000);
		this.notifyFinish(15, null);		
	}
}

@IAsyncNodeSchdule(Default = 3, Ready = 1, Destryable = 0, Destryed = 0, NotifiedInput = 0, NotifiedOutput = 0, NotifyFinish = 0, Pending = 0, Running = 0)
class EB extends TestElementBase
{
	public EB()
	{}
	
	@Override
	public void executeCore() throws Exception {
		
		Thread.sleep(1000);
		this.notifyFinish(7, null);
	}
}

class EC extends TestElementBase
{
	public EC()
	{}
	
	@Override
	public void executeCore() throws Exception {
		
		Thread.sleep(1000);
		this.notifyFinish(6, null);
	}
}

class EE extends TestElementBase
{
	public EE()
	{}
	
	@Override
	public void executeCore() throws Exception {
		
		Thread.sleep(1000);
		this.notifyFinish(3, null);
	}
}

class ED extends TestElementBase
{
	public ED()
	{}
	
	@Override
	public void executeCore()  throws Exception {
		
		Thread.sleep(1000);
		this.notifyFinish(2, null);
	}
}

class EDE extends TestElementBase
{
	public EDE()
	{}
	
	public static final String AttrD = "D";
	public static final String AttrE = "E";
	
	public int getD() throws Exception
	{
		return ((Integer)this.getAttribute(AttrD)).intValue();
	}
	
	public int getE() throws Exception
	{
		return ((Integer)this.getAttribute(AttrE)).intValue();
	}
	
	@Override
	public void executeCore() throws Exception {
		
		Thread.sleep(1000);
		this.notifyFinish(this.getD() + this.getE(), null);
	}
}

class EBC extends TestElementBase
{
	public EBC()
	{}
	
	public static final String AttrB = "B";
	public static final String AttrC = "C";
	
	public int getB() throws Exception
	{
		return ((Integer)this.getAttribute(AttrB)).intValue();
	}
	
	public int getC() throws Exception
	{
		return ((Integer)this.getAttribute(AttrC)).intValue();
	}
	
	@Override
	public void executeCore() throws Exception {
		
		Thread.sleep(1000);
		this.notifyFinish(this.getB() * 3 - this.getC(), null);
	}
}

abstract class TestElementBase extends ElementBase
{
	public static int executeCount = 0;
	public static boolean isPersistent = true;
	
	@Override
	protected void execute() throws Exception
	{	
		executeCount++;
		super.execute();
	}	
	
	@Override
	public boolean isPersistent()
	{
		return isPersistent;
	}
	
	@Override
	public void notifyFinish(Object value, ConcurrentMap<String, Object> appVariables, ElementBuilder elementBuilder) throws Exception
	{
		executeCount--;
		super.notifyFinish(value, appVariables, elementBuilder);
	}	
}

class EResult extends TestElementBase
{
	public EResult() throws Exception
	{
		/*
		this.setVariable(AttrDE, new Date().getTime());
		logger.info("user defined variable 1 is :" + this.getVariable(AttrDE));
		*/
	}
	
	public static final String AttrA = "A";
	public static final String AttrBC = "BC";
	public static final String AttrDE = "DE";
	
	public int getA() throws Exception
	{
		return ((Integer)this.getAttribute(AttrA)).intValue();
	}

	public int getBC() throws Exception
	{
		return ((Integer)this.getAttribute(AttrBC)).intValue();
	}
	
	public int getDE() throws Exception
	{
		return ((Integer)this.getAttribute(AttrDE)).intValue();
	}
	
	@Override
	public void executeCore() throws Exception {
		Thread.sleep(1000);

		UUID uuid = this.getWorkFlowID();
		ElementBuilder elementBuilder = new ElementBuilder(uuid);
		elementBuilder.add("Dyn2", new Dyn2());
		elementBuilder.add("Dyn3", new Dyn3());
		elementBuilder.add("DynResult", new DynResult());
		elementBuilder.addMap("DynResult", "v2", "Dyn2");
		elementBuilder.addMap("DynResult", "v3", "Dyn3");	
		this.notifyFinish(getA() + getBC() / getDE(), null, elementBuilder);	
	}
}

