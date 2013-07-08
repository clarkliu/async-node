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

public class ServiceResultElement extends ElementBase
{	
	public ServiceResultElement() throws Exception
	{
		this.setVariable("MarkDelete", false);
	}
	
	@Override
	public boolean reCallingAble()
	{
		return true;
	}
	
	public AsyncRequestResultContainer markDelete() throws Exception
	{
		this.setVariable("MarkDelete", true);			
		AsyncRequestResultContainer result = new AsyncRequestResultContainer();
		result.setStep(ServiceProcessStep.Deleted);
		return result;
	}
	
	public AsyncRequestResultContainer getResult() throws Exception
	{
		return (AsyncRequestResultContainer)this.getAttribute("ServiceResultAttribute");
	}
	
	@Override
	public void executeCore() throws Exception {
		// TODO Auto-generated method stub			
		Boolean markDelete = (Boolean)this.getVariable("MarkDelete");
		if(markDelete != null && markDelete.booleanValue())
		{
			this.notifyFinish(null, null);
		}
		else
		{
			this.notifyRecalling();
		}
	}		
}