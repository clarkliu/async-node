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

import asyncnode.rpc.AsyncRequestResultContainer;

public class ClientResultContainer
{
	protected AsyncRequestResultContainer requestResult;
	protected UUID elementID;
	
	public AsyncRequestResultContainer getRequestResult() {
		return requestResult;
	}
	public void setRequestResult(AsyncRequestResultContainer requestResult) {
		this.requestResult = requestResult;
	}
	public UUID getElementID() {
		return elementID;
	}
	public void setElementID(UUID elementID) {
		this.elementID = elementID;
	}
	
}