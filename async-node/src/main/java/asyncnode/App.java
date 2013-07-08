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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import asyncnode.implement.CassandraAsyncNodeDao;
import asyncnode.implement.CassandraAsyncQueueDao;
import asyncnode.implement.CassandraWorkFlowDao;
import asyncnode.implement.ElementProvider;
import asyncnode.implement.MemoryAsyncNodeDao;
import asyncnode.implement.MemoryAsyncQueueDao;
import asyncnode.implement.StoreableElementDao;
import asyncnode.implement.cassandra.CassandraHelper.CassandraDescrib;

import org.apache.log4j.Logger;

public class App {
	private static Injector config = null;
	
	public static void initConfig() throws Exception
	{
		InitConfigSetting initConfigSetting = new InitConfigSetting();
		initConfigSetting.setInitPool(true);
		initConfigSetting.setInitStorage(true);
		initConfig(new DefaultAppModule(), initConfigSetting);
	}
	
	public static class DefaultAppModule extends AbstractModule
	{
		@Override
		protected void configure() {
			AppContext appContext = new AppContext();
			appContext.setMaxThreadCount(100);
			appContext.setSleepInterval(1000);
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
			
			this.bind(Logger.class).toInstance(Logger.getLogger(App.class));
			this.bind(ExceptionHandler.class).toInstance(new ExceptionHandler());
			//this.bind(StoreableElementDao.class)				
		}	
	}
	
	public static void initConfig(AbstractModule module, InitConfigSetting initConfigSetting) throws Exception
	{
		config = Guice.createInjector(module);
		if(initConfigSetting == null)
		{
			AsyncHelper.initPool();
			config.getInstance(StoreableElementDao.class).initAllStorage();
		}
		else
		{
			if(initConfigSetting.getInitPool())
			{
				AsyncHelper.initPool();
			}
			
			if(initConfigSetting.getInitStorage())
			{
				config.getInstance(StoreableElementDao.class).initAllStorage();
			}
		}
	}	
	
	public static Injector getConfig()
	{
		return config;
	}
	
	public static class InitConfigSetting
	{
		public boolean initPool;
		public boolean initStorage;
		public boolean getInitPool() {
			return initPool;
		}
		public void setInitPool(boolean initPool) {
			this.initPool = initPool;
		}
		public boolean getInitStorage() {
			return initStorage;
		}
		public void setInitStorage(boolean initStorage) {
			this.initStorage = initStorage;
		}
	}	
	
	public static class AppContext
	{
		private int maxThreadCount;
		private int sleepInterval;	 
		private int defaultSchduleCycle;	
		
		public int getMaxThreadCount() {
			return maxThreadCount;
		}
		public void setMaxThreadCount(int maxThreadCount) {
			this.maxThreadCount = maxThreadCount;
		}

		public int getSleepInterval() {
			return sleepInterval;
		}
		public void setSleepInterval(int sleepInterval) {
			this.sleepInterval = sleepInterval;
		}
		public int getDefaultSchduleCycle() {
			return defaultSchduleCycle;
		}
		public void setDefaultSchduleCycle(int defaultSchduleCycle) {
			this.defaultSchduleCycle = defaultSchduleCycle;
		}
	}		
}