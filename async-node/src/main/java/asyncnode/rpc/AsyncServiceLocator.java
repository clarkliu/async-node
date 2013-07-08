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

package asyncnode.rpc;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;

import com.google.inject.Inject;

import asyncnode.rpc.AsyncServiceModule;
import asyncnode.App;
import asyncnode.ElementBase;
import asyncnode.ElementBuilder;
import asyncnode.rpc.ServiceProcessElement;
import asyncnode.rpc.ServiceResultElement;
import asyncnode.implement.StoreableElementDao;
import asyncnode.rpc.AsyncRequestResultContainer;
import asyncnode.rpc.RPCCallDescrib;
import asyncnode.rpc.RPCMethod;
import asyncnode.rpc.ServiceProcessStep;

public class AsyncServiceLocator
{
	protected ConcurrentMap<String, Class> selfServiceMap = new ConcurrentHashMap<String, Class>();
	protected StoreableElementDao storeableElementDao;
	
	@Inject
	public void setStoreableElementDao(StoreableElementDao storeableElementDao) {
		this.storeableElementDao = storeableElementDao;
	}

	
	public void publish(ConcurrentHashMap<String, Class> serviceMap)
	{
		selfServiceMap.putAll(serviceMap);			
	}
	
	public Object activate(RPCCallDescrib describ, Object param) throws Exception
	{
		if(describ.getAction().endsWith(RPCMethod.Post.toString()))
		{
			UUID workFlowID = UUID.randomUUID();
			ServiceResultElement resultElement = new ServiceResultElement();
			resultElement.setId(workFlowID);
			ElementBase elementBase = ((Class<ServiceProcessElement>)selfServiceMap.get(describ.getUrl())).newInstance();
			((ServiceProcessElement)elementBase).initParam(param);
			ElementBuilder elementBuilder = new ElementBuilder(workFlowID);
			elementBuilder.add("process", elementBase);
			elementBuilder.add("result", resultElement);
			elementBuilder.addMap("result", "ServiceResultAttribute", "process");
			elementBuilder.build();
			App.getConfig().getInstance(Logger.class).info("...............after service build!" + "," + workFlowID + "," + resultElement.getId());
			return workFlowID;
		}			
		else
		{
			ServiceResultElement resultElement = ((ServiceResultElement)storeableElementDao.getElementBase(param.toString()));
			if(resultElement == null)
			{
				AsyncRequestResultContainer result = new AsyncRequestResultContainer();
				result.setStep(ServiceProcessStep.Deleted);
				return result;
			}
			
			if(describ.getAction().equals(RPCMethod.Delete.toString()))
			{
				return resultElement.markDelete();
			}
			else
			{
				return resultElement.getResult();
			}
		}
	}
}
