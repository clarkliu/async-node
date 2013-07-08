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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;

import asyncnode.implement.CassandraAsyncNodeDao;
import asyncnode.implement.MemoryAsyncNodeDao;
import asyncnode.implement.MemoryAsyncQueueDao;
import asyncnode.implement.StoreableElementDao;
import asyncnode.implement.CassandraWorkFlowDao;
import asyncnode.implement.ElementProvider;
import asyncnode.implement.ThreadProxy;
import asyncnode.core.IAction;
import asyncnode.core.ICreateInstance;

public class AsyncNodeRunner {	
	private static ExecutorService executorService = null;
	private static ThreadProxy threadProxy = null;
	
	static
	{
		executorService = Executors.newSingleThreadExecutor();
		threadProxy = new ThreadProxy();
	}
	
	public static void run(final IAction emptyLoopAction, final IAction oneLoopAction) throws Exception
	{
		threadProxy.setiAction(new IAction(){
			public void invoke(Object tParam) throws Exception {
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
						if(emptyLoopAction != null)
						{
							emptyLoopAction.invoke(null);
						}
					}	
					if(oneLoopAction != null)
					{
						oneLoopAction.invoke(null);
					}					
				}
			}});
		executorService.execute(threadProxy);	
	}
	
	public static boolean runByPriority() throws Exception
	{
		StoreableElementDao cassandraElementDao = App.getConfig().getInstance(StoreableElementDao.class);
		boolean canRun = cassandraElementDao.runByPriority();
		if(canRun)
		{
			App.getConfig().getInstance(Logger.class).info("finish one loop");
		}
		else
		{
			App.getConfig().getInstance(Logger.class).info("finish one empty loop");
		}
		return canRun;
	}
		
	public static int getSchduleCycle(ElementStatusEnum status, ElementBase elementBase) throws Exception
	{		
		IAsyncNodeSchdule schdule = elementBase.getClass().getAnnotation(IAsyncNodeSchdule.class);
		int defaultSchduleCycle = App.getConfig().getInstance(App.AppContext.class).getDefaultSchduleCycle();
		if(schdule != null)
		{
			int defaultValue = schdule.Default();
			if(defaultValue <= 0)
			{
				defaultValue = defaultSchduleCycle;
			}
			
			if(status.equals(ElementStatusEnum.Destryable))
			{
				return schdule.Destryable() > 0 ? schdule.Destryable() : defaultValue;
			}
			else if(status.equals(ElementStatusEnum.Destryed))
			{
				return schdule.Destryed() > 0 ? schdule.Destryed() : defaultValue;
			}
			else if(status.equals(ElementStatusEnum.NotifiedInput))
			{
				return schdule.NotifiedInput() > 0 ? schdule.NotifiedInput() : defaultValue;
			}
			else if(status.equals(ElementStatusEnum.NotifiedOutput))
			{
				return schdule.NotifiedOutput() > 0 ? schdule.NotifiedOutput() : defaultValue;
			}
			else if(status.equals(ElementStatusEnum.NotifyFinish))
			{
				return schdule.NotifyFinish() > 0 ? schdule.NotifyFinish() : defaultValue;
			}
			else if(status.equals(ElementStatusEnum.Pending))
			{
				return schdule.Pending() > 0 ? schdule.Pending() : defaultValue;
			}
			else if(status.equals(ElementStatusEnum.Ready))
			{
				return schdule.Ready() > 0 ? schdule.Ready() : defaultValue;
			}
			else if(status.equals(ElementStatusEnum.Running))
			{
				return schdule.Running() > 0 ? schdule.Running() : defaultValue;
			}
			else
			{
				return defaultValue;
			}
		}
		else
		{
			return defaultSchduleCycle;
		}
	}
}
