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

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import asyncnode.implement.StoreableElementDao;
import asyncnode.rpc.AsyncServiceLocator;
import asyncnode.rpc.RPCClientDeleteElement;
import asyncnode.rpc.RPCClientResultElement;
import asyncnode.rpc.RPCRequestElement;


public class ElementBuilder {
	protected UUID workFlowID;
	protected ConcurrentMap<String, ElementBase> elementMapping = new ConcurrentHashMap<String, ElementBase>();
	protected ConcurrentMap<String, List<String[]>> relationMapping = new ConcurrentHashMap<String, List<String[]>>();
	
	public ElementBuilder(UUID workFlowID)
	{	
		this.workFlowID = workFlowID;
	}
	
	protected List<String> getNoChildNodes()
	{
		Set<String> parentNodes = getAllParentNodes();
		List<String> noChildrenNodes = new ArrayList<String>();
		for(Entry<String, ElementBase> elementEntry : elementMapping.entrySet())
		{
			if(!parentNodes.contains(elementEntry.getKey()))
			{
				noChildrenNodes.add(elementEntry.getKey());
			}
		}
		return noChildrenNodes;
	}
	
	protected Set<String> getAllParentNodes()
	{
		Set<String> parentSet = new HashSet<String>();
		for(Entry<String, List<String[]>> relationEntry : relationMapping.entrySet())
		{
			if(!parentSet.contains(relationEntry.getKey()))
			{
				parentSet.add(relationEntry.getKey());
			}
		}
		return parentSet;
	}
	
	protected Set<String> getAllChildrenNodes()
	{
		Set<String> childrenSet = new HashSet<String>();
		for(Entry<String, List<String[]>> relationEntry : relationMapping.entrySet())
		{
			for(String[] childNameArray : relationEntry.getValue())
			{
				String childName = childNameArray[0];
				if(!childrenSet.contains(childName))
				{
					childrenSet.add(childName);
				}
			}
		}
		return childrenSet;
	}
	
	protected List<String> getNoParentNodes()
	{		
		Set<String> childrenSet = getAllChildrenNodes();
		List<String> noParentNodes = new ArrayList<String>();
		for(Entry<String, ElementBase> elementEntry : elementMapping.entrySet())
		{
			if(!childrenSet.contains(elementEntry.getKey()))
			{
				noParentNodes.add(elementEntry.getKey());
			}
		}
		return noParentNodes;
	}
	
	public ElementBuilder add(String elementName, ElementBase elementBase)
	{
		elementBase.setWorkFlowID(workFlowID);
		elementMapping.put(elementName, elementBase);
		return this;
	}

	public static void buildClient(String url, Object param, ElementBase callBackElement) throws Exception 
	{
		RPCRequestElement rpcRequest = new RPCRequestElement(url, param);
		buildClient(url, rpcRequest, callBackElement);
	}
	
	public static void buildClient(String url, ElementBase requestElement, ElementBase callBackElement) throws Exception
	{
		ElementBuilder elementBuilder = new ElementBuilder(UUID.randomUUID());
		elementBuilder.add("request", requestElement);
		elementBuilder.add("result", new RPCClientResultElement(url));
		elementBuilder.add("delete", new RPCClientDeleteElement(url));
		elementBuilder.add("callback", callBackElement);
		elementBuilder.addMap("result", "ResultElementID", "request");
		elementBuilder.addMap("callback", "Result", "result");
		elementBuilder.addMap("delete", "ClientNodeResult", "result");			
		elementBuilder.build();			
	}	
	
	public static void registerService(ConcurrentHashMap<String, Class> serviceMap)
	{		
		App.getConfig().getInstance(AsyncServiceLocator.class).publish(serviceMap);
	}	
	
	public ElementBuilder addMap(String parentName, String attributeName, 
			String childName)
	{
		List<String[]> childList = null;
		if(!relationMapping.containsKey(parentName))
		{
			childList = new ArrayList<String[]>();
			relationMapping.put(parentName, childList);
		}
		else
		{
			childList = relationMapping.get(parentName);
		}
		childList.add(new String[]{ childName, attributeName });
		return this;
	}
	
	public static void buildDynamic(ElementBase element, ElementBuilder elementBuilder) throws Exception
	{
		elementBuilder.buildRelation();
		List<String> noChildNodesList = elementBuilder.getNoChildNodes();
		for(int nodeI = 0; nodeI < noChildNodesList.size(); nodeI++)
		{
			String noChildNode = noChildNodesList.get(nodeI);
			ElementBase noChildElement = elementBuilder.elementMapping.get(noChildNode);
			noChildElement.addElementCore(element, ElementBase.DynamicAttributeName, true);
		}
		elementBuilder.saveNode();
	}
	
	protected void buildRelation() throws Exception
	{
		for(Entry<String, List<String[]>> relationEntry : relationMapping.entrySet())
		{
			String parentName = relationEntry.getKey();
			List<String[]> childNameList = relationEntry.getValue();
			ElementBase parentElement = elementMapping.get(parentName);
			for(String[] nameArray : childNameList)
			{
				String childName = nameArray[0];
				String attributeName = nameArray[1];
				parentElement.addElement(elementMapping.get(childName), attributeName);
			}
		}
	}
	
	protected void saveNode() throws Exception
	{
		List<String> noParentNodes = null;
		List<ElementBase> toAddList = new ArrayList();
		while((noParentNodes = getNoParentNodes()).size() > 0)
		{
			for(String parentNode : noParentNodes)
			{
				ElementBase element = elementMapping.get(parentNode);
				element.build();
				relationMapping.remove(parentNode);
				elementMapping.remove(parentNode);
				toAddList.add(element);
			}
		}
		App.getConfig().getInstance(StoreableElementDao.class).addBufferElementList(toAddList);
	}
	
	public void build() throws Exception
	{				
		buildRelation();		
		saveNode();
	}	
}
