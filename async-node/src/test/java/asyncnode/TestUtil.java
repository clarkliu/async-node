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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Test;

import asyncnode.core.IAction;
import asyncnode.core.IFunc;
import asyncnode.implement.ThreadProxy;

public class TestUtil {
	private static Logger logger = Logger.getLogger(TestUtil.class);
	private static ExecutorService pool = null;	
	
	private static class TestActionCall implements IAction
	{
		private int i1;
		public TestActionCall(int i1)
		{
			this.i1 = i1;
		}
		
		public void invoke(Object tParam) throws Exception {
			logger.info("single call " + this.i1 + " begin");
			Thread.sleep(4000);
			logger.info("single call " + this.i1 + " end");
		}		
	}
	
	private static class TestConcurrency
	{
		public synchronized void test2() throws Exception
		{
			for(int i1 = 0; i1 < 7; i1++)
			{
				logger.info("test2");
				Thread.sleep(1000);
			}
		}
		
		public synchronized void test1() throws Exception
		{
			for(int i1 = 0; i1 < 7; i1++)
			{
				logger.info("test1");
				Thread.sleep(1000);
			}
		}
	}
	
	public void asyncInvoke2(IAction action) throws Exception
	{
		if(pool == null)
		{
			pool = new ThreadPoolExecutor(3, 10,
	                60L, TimeUnit.SECONDS,
	                new SynchronousQueue<Runnable>());
		}
		
		ThreadProxy threadHelper = new ThreadProxy();
		threadHelper.setiAction(action);
		pool.execute(threadHelper);
	}
	
	//@Test
	public void testLog()
	{
		
		logger.info(".........get logger 1!");
		logger.info(".........get logger 2!");
		logger.warn("a warning message 1");
		logger.warn("a warning message 2");
		logger.warn("a warning message 3");
		logger.warn("a warning message 4");
		
		Logger logger = Logger.getLogger(TestAsyncElement.class);
		logger.info("..........write log!......");
	}

	private static class printList implements IFunc<List<String>, List<String>>
	{
		public List<String> getValue(List<String> strList) throws Exception {
			
			logger.info("............");
			for(String str : strList)
			{ 
				logger.info(str);
			}
			
			return strList;
		}

		//@Test
		public void testLargeThread()
		{
			ExecutorService executorService = Executors.newCachedThreadPool();
			for(int i1 = 0; i1 < 2000; i1++)
			{
				ThreadProxy threadProxy = new ThreadProxy();
				threadProxy.setiAction(new TestActionCall(i1));
				executorService.execute(threadProxy);
			}
		}		
	}
	
	//@Test
	public  void testPagedList()
	{
		List<String> strList = new ArrayList<String>();
		for(int i1 = 0; i1 < 6; i1++)
		{
			strList.add(String.valueOf(i1));
		}
		try {
			for(int i1 = 1; i1 <= 5; i1++)
			{
				strList = CommonUtil.getPagedList(strList, i1, new printList());
				logger.info("!!!!!!!!!!!!!!\n\n\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//@Test
	public void testConcurrency() throws Exception
	{
		final TestConcurrency testConcurrency = new TestConcurrency();
		asyncInvoke2(new IAction(){
			public void invoke(Object tParam) throws Exception {
				testConcurrency.test1();
			}});
		
		asyncInvoke2(new IAction(){
			public void invoke(Object tParam) throws Exception {
				testConcurrency.test2();
			}});		
	}
	
	//@Test
	public void testLargeFixThread()
	{
		ExecutorService executorService = Executors.newFixedThreadPool(1000);
		for(int i1 = 0; i1 < 2000; i1++)
		{
			ThreadProxy threadProxy = new ThreadProxy();
			threadProxy.setiAction(new TestActionCall(i1));
			executorService.execute(threadProxy);
		}
	}
	
	//@Test
	public void testSingleThead()
	{
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		ThreadProxy threadProxy = new ThreadProxy();
		threadProxy.setiAction(new IAction(){
			public void invoke(Object tParam) throws Exception {
				logger.info("single call 1 begin");
				Thread.sleep(4000);
				logger.info("single call 1 end");
			}});
		executorService.execute(threadProxy);
		
		logger.info("after call async1");
		executorService.execute(threadProxy);
	}
}
