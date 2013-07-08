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

import asyncnode.ElementBase;
import asyncnode.rpc.AsyncRequestResultContainer;
import asyncnode.rpc.ServiceProcessStep;

public abstract class ServiceProcessElement extends ElementBase
{
	public ServiceProcessElement()
	{
		AsyncRequestResultContainer result = new AsyncRequestResultContainer();
		result.setStep(ServiceProcessStep.Created);
		this.value = result;
	}
	
	public void initParam(Object param) throws Exception
	{
		this.setVariable("DelegateActionParam", param);
	}
	
	protected abstract Object process(Object param) throws Exception;
	
	@Override
	public void executeCore() throws Exception {
		// TODO Auto-generated method stub
		Object param = this.getVariable("DelegateActionParam");
		Object tValue = process(param);
		AsyncRequestResultContainer result = new AsyncRequestResultContainer();
		result.setStep(ServiceProcessStep.Processed);
		result.setValue(tValue);
		this.notifyFinish(result, null);
	}		
}