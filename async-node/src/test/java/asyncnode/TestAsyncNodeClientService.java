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
import java.util.UUID;

import org.apache.log4j.Logger;

import asyncnode.App;
import asyncnode.ElementBase;
import asyncnode.ElementBuilder;

import asyncnode.implement.StoreableElementDao;
import asyncnode.core.IAction;
import asyncnode.core.ICreateInstance;
import asyncnode.rpc.AsyncServiceLocator;
import asyncnode.rpc.AsyncServiceModule;
import asyncnode.rpc.ClientResultContainer;
import asyncnode.rpc.RPCClientResultElement;
import asyncnode.rpc.ServiceProcessElement;

public class TestAsyncNodeClientService {
	public static Logger logger = Logger.getLogger(Logger.class);
	private static App iocContainer = new App();	
	public static class TestProcessServiceElement extends ServiceProcessElement
	{
		public TestProcessServiceElement()
		{}
		
		@Override
		public Boolean schdule() throws Exception
		{
			return super.schdule();
		}
		
		@Override
		protected Object process(Object param) throws Exception {
			logger.info("param is:" + param);
			return Integer.parseInt(param.toString()) * 10;
		}
	}
}
