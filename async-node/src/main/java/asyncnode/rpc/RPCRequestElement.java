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

import asyncnode.ElementBase;
import asyncnode.rpc.RPCClientResultElement;
import asyncnode.rpc.RPCMethod;

public class RPCRequestElement extends ElementBase
{
	public RPCRequestElement()
	{}
	
	public RPCRequestElement(String url, Object param) throws Exception
	{
		this.setVariable("RequestUrlVariable", url);
		this.setVariable("RequestParamVariable", param);
	}
	
	@Override
	public void executeCore() throws Exception {
		// TODO Auto-generated method stub
		Object param = this.getVariable("RequestParamVariable");
		UUID elementID = (UUID)RPCClientResultElement.callService((String)this.getVariable("RequestUrlVariable"), RPCMethod.Post, param);
		this.notifyFinish(elementID, null);		
	}		
}