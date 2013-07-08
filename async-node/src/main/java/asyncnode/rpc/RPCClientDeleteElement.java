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

import org.apache.log4j.Logger;

import asyncnode.App;
import asyncnode.ElementBase;
import asyncnode.rpc.AsyncRequestResultContainer;
import asyncnode.rpc.ClientResultContainer;
import asyncnode.rpc.RPCMethod;
import asyncnode.rpc.ServiceProcessStep;

public class RPCClientDeleteElement extends ElementBase
{
	public RPCClientDeleteElement()
	{}
	
	public RPCClientDeleteElement(String url) throws Exception
	{
		this.setVariable("RequestUrlVariable", url);
	}
	
	@Override
	public void executeCore() throws Exception {
		App.getConfig().getInstance(Logger.class).info("...........delete prc client");
		ClientResultContainer clientResult = (ClientResultContainer)this.getAttribute("ClientNodeResult");
		AsyncRequestResultContainer resultContainer = clientResult.getRequestResult();
		if(resultContainer != null
				&& (resultContainer.getStep().equals(ServiceProcessStep.Created) 
						|| resultContainer.getStep().equals(ServiceProcessStep.Processed)))
		{
			RPCClientResultElement.callService((String)this.getVariable("RequestUrlVariable"), RPCMethod.Delete, clientResult.getElementID());
		}
		this.notifyFinish(null, null);
	}		
}	
