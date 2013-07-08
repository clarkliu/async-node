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

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.inject.Singleton;

import asyncnode.implement.ThreadProxy;
import asyncnode.core.IAction;

@Singleton
public class AsyncHelper {
	private static ExecutorService pool = null;	
	public static void initPool()
	{
		App.AppContext asyncContext = App.getConfig().getInstance(App.AppContext.class);
		pool = Executors.newFixedThreadPool(asyncContext.getMaxThreadCount());
	}

	public static void asyncInvoke(IAction action) throws Exception
	{
		ThreadProxy threadHelper = new ThreadProxy();
		threadHelper.setiAction(action);
		pool.execute(threadHelper);
	}
}
