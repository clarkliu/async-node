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

import com.google.inject.Inject;

import asyncnode.App;
import asyncnode.ElementBase;
import asyncnode.ElementBuilder;
import asyncnode.rpc.RPCRequestElement;
import asyncnode.rpc.AsyncRequestResultContainer;
import asyncnode.rpc.AsyncServiceLocator;
import asyncnode.rpc.ClientResultContainer;
import asyncnode.rpc.RPCCallDescrib;
import asyncnode.rpc.RPCClientDeleteElement;
import asyncnode.rpc.RPCClientResultElement;
import asyncnode.rpc.RPCMethod;
import asyncnode.rpc.RequestContentType;
import asyncnode.rpc.ServiceProcessStep;

public class RPCClientResultElement extends ElementBase
{	
	public RPCClientResultElement()
	{}
	
	public RPCClientResultElement(String url) throws Exception
	{
		this.setVariable("RequestUrlVariable", url);
	}
	
	public static Object callService(String url, RPCMethod method, Object param) throws Exception
	{
		RPCCallDescrib rpcCallDescrib = new RPCCallDescrib();
		rpcCallDescrib.setUrl(url);
		rpcCallDescrib.setContentType(RequestContentType.Json);
		rpcCallDescrib.setMethod(method);
		return App.getConfig().getInstance(AsyncServiceLocator.class).activate(rpcCallDescrib, param);
	}
	
	@Override
	public void executeCore() throws Exception {
		UUID elementID = (UUID)this.getAttribute("ResultElementID");
		AsyncRequestResultContainer resultContainer = (AsyncRequestResultContainer)callService((String)this.getVariable("RequestUrlVariable"), 
				RPCMethod.Get, elementID);
		if(resultContainer != null
				&& (resultContainer.getStep().equals(ServiceProcessStep.Processed) 
						|| resultContainer.getStep().equals(ServiceProcessStep.Deleted)))
		{
			ClientResultContainer clientResultContainer = new ClientResultContainer();
			clientResultContainer.setElementID(elementID);
			clientResultContainer.setRequestResult(resultContainer);
			this.notifyFinish(clientResultContainer, null);
		}
		else
		{
			this.notifyRecalling();
		}
	}
			
	@Override
	public boolean reCallingAble()
	{
		return true;
	}		
}
