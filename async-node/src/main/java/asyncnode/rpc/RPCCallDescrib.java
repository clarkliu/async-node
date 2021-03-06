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

import asyncnode.rpc.RPCMethod;
import asyncnode.rpc.RequestContentType;

public class RPCCallDescrib
{
	protected RPCMethod method;
	protected RequestContentType contentType;
	protected String url;
	
	public RPCCallDescrib()
	{
		contentType = RequestContentType.Json;
	}
	
	public RPCMethod getMethod() {
		return method;
	}
	public void setMethod(RPCMethod method) {
		this.method = method;
	}
	public RequestContentType getContentType() {
		return contentType;
	}
	public void setContentType(RequestContentType contentType) {
		this.contentType = contentType;
	}
	
	public String getAction() {
		return method.toString();
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}		
}