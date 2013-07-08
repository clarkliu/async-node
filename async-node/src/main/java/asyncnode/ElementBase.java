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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;
import java.util.UUID;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import asyncnode.implement.ElementProvider;
import asyncnode.implement.StoreableElementDao;
import asyncnode.implement.cassandra.FieldSerailzerBase;
import asyncnode.implement.cassandra.ICassandraField;
import asyncnode.implement.cassandra.ICassandraKeyField;
import asyncnode.core.IAction;


@ICassandraKeyField(KeyFieldName = "id")
@IAsyncNodeSchdule(Default = 1, Ready = 2, Destryable = 0, Destryed = 0, NotifiedInput = 0, NotifiedOutput = 0, NotifyFinish = 0, Pending = 0, Running = 0)
public abstract class ElementBase {		
	public static final String DynamicAttributeName = "DynamicAttribute";
	
	public static final String AppPrefix = "App";
	public static final String SysPrefix = "Sys";
	
	public static final String ClassNameField = SysPrefix + "_ClassName";
	
	public static final String StatusField = SysPrefix + "_Status";
	public static final String LastEditDateField = SysPrefix + "_LastEditDate";	
	public static final String WorkFlowIDField = SysPrefix + "_WorkFlowID";
	public static final String IDField = SysPrefix + "_ID";
	public static final String ValueField = SysPrefix + "_ValueField" + ElementBase.nameSeperator;
	public static final String ValueClass = SysPrefix + "_ValueClass" + ElementBase.nameSeperator;
	public static final String AppVariableValuePrefix = SysPrefix + "_AppVarValue" + ElementBase.nameSeperator;
	public static final String AppVariableClassPrefix = SysPrefix + "_AppVarClass" + ElementBase.nameSeperator;
	
	public static final String InNodesValuePrefix = SysPrefix + "_InNodesValue" + ElementBase.nameSeperator;
	public static final String InNodesClassPrefix = SysPrefix + "_InNodesClass" + ElementBase.nameSeperator;
	public static final String OutNodesValuePrefix = SysPrefix + "_OutNodesValue" + ElementBase.nameSeperator;
	public static final String OutNodesClassPrefix = SysPrefix + "_OutNodesClass" + ElementBase.nameSeperator;
	public static final String AttributeValuePrefix = SysPrefix + "_AttributeValue" + ElementBase.nameSeperator;
	public static final String AttributeClassPrefix = SysPrefix + "_AttributeClass" + ElementBase.nameSeperator;
	
	public static CharSequence nameSeperator = "|";

	protected StoreableElementDao iElementDao = App.getConfig().getInstance(StoreableElementDao.class);
	
	@ICassandraField(ColumnName = ElementBase.WorkFlowIDField, Serialer = FieldSerailzerBase.class)
	protected UUID workFlowID;
	
	@ICassandraField(ColumnName = ElementBase.IDField, Serialer = FieldSerailzerBase.class)
	protected UUID id;
	
	@ICassandraField(ColumnName = ElementBase.LastEditDateField, Serialer = FieldSerailzerBase.class)
	protected Date lastEditDate = null;
	
	@ICassandraField(ColumnName = ElementBase.StatusField, Serialer = FieldSerailzerBase.class)
	protected ElementStatusEnum status = ElementStatusEnum.New;
	
	protected ConcurrentMap<String, Boolean> inNodesMap = new ConcurrentHashMap<String, Boolean>();	
	protected ConcurrentMap<String, Boolean> outNodesMap = new ConcurrentHashMap<String, Boolean>();			
	protected ConcurrentMap<String, String> attributeMap = new ConcurrentHashMap<String, String>();		
	public Object value = null;	
	public ConcurrentMap<String, Object> appVariables = new ConcurrentHashMap<String, Object>();	
	
	public void doStartupResotre() throws Exception
	{
		if(status.equals(ElementStatusEnum.Running))
		{
			this.notifyRecalling();
		}
	}
	
	protected boolean reCallingAble()
	{
		return false;
	}

	public static boolean isValidateName(String name)
	{
		if(name != null && name.length() > 0)
		{
			if(!name.contains(nameSeperator))
			{
				return true;
			}
		}
		return false;
	}
	
	public static void checkNameValidate(String name) throws Exception
	{
		if(!isValidateName(name))
		{
			throw new Exception("variable name can't be null or empty or contains " + nameSeperator);
		}
	}
	
	public ElementBase()
	{
		this.id = UUID.randomUUID();
	}
	
	protected Object getVariable(String variableKey)
	{		
		return appVariables.get(variableKey);
	}
	
	protected void setVariable(String variableKey, Object variableValue) throws Exception
	{		
		checkNameValidate(variableKey);
		appVariables.put(variableKey, variableValue);
		String[] strArray = new String[]{ variableKey };
		this.submitAppVariables(strArray);
	}
	
	protected void submitAppVariables(String[] appVariables) throws Exception
	{
		ConcurrentMap<String, Object> varMap = new ConcurrentHashMap<String, Object>();
		for(String appName : appVariables)
		{
			Object appValue = this.appVariables.get(appName);
			varMap.put(appName, appValue);
		}
		iElementDao.submitAppVariables(this, varMap);
	}
	
	public Object getAttribute(String attributeName) throws Exception
	{
		if(!attributeMap.containsKey((attributeName)))
		{
			return null;
		}
		return iElementDao.getAttribute(attributeMap.get(attributeName));
	}

	public Boolean isNode()
	{
		return inNodesMap.size() > 0;
	}
	
	protected void addElement(ElementBase element, String attributeName) throws Exception
	{
		addElementCore(element, attributeName, false);
	}
	
	protected void addElementCore(ElementBase element, String attributeName, boolean inNodeFinished) throws Exception
	{
		checkNameValidate(attributeName);
		checkNameValidate(this.id.toString());
		checkNameValidate(element.id.toString());
		
		attributeMap.put(attributeName, element.id.toString());
		element.outNodesMap.put(this.id.toString(), false);
		this.inNodesMap.put(element.id.toString(), inNodeFinished);
	}
	
	public String getClassName()
	{
		return this.getClass().getName();
	}
	
	public static ElementBase getElementInstance(String className) 
			throws Exception
	{
		ElementBase elementBase = App.getConfig().getInstance(ElementProvider.class)
				.getInstance(className);
		return elementBase;
	}
	
	public void build() throws Exception
	{
		iElementDao.addElement(this);
	}
	
	public Boolean canSchdule()
	{
		if(status.equals(ElementStatusEnum.Running)
			|| status.equals(ElementStatusEnum.Destryed))
		{
			return false;
		}
		return true;
	}
		
	protected Boolean canExecute()
	{
		return true;
	}
	
	public Boolean schdule() throws Exception
	{
		Boolean isEnded = false;
		if(status.equals(ElementStatusEnum.Pending))
		{
			App.getConfig().getInstance(Logger.class).info("pending node exchange to ready........!");			
			iElementDao.setReady(this);	
		}
		else if(status.equals(ElementStatusEnum.Ready))
		{
			App.getConfig().getInstance(Logger.class).info("now execute node:" + this.id.toString()+"," + this.getClass().getName());
			if(canExecute())
			{
				execute();
			}
		}	
		else if(status.equals(ElementStatusEnum.Running))
		{
			App.getConfig().getInstance(Logger.class).info("task is already running!");
		}
		else if(status.equals(ElementStatusEnum.NotifyFinish))
		{
			notifyOuput();		
		}			
		else if(status.equals(ElementStatusEnum.NotifiedOutput))
		{
			notifyInput();
		}
		else if (status.equals(ElementStatusEnum.NotifiedInput))
		{			
			App.getConfig().getInstance(Logger.class).info("NotifiedInput node exchange to Destryable........!");			
			iElementDao.setDestryable(this);
		}
		else if(status.equals(ElementStatusEnum.Destryable))
		{
			destroy();
			isEnded = true;			
		}
		else if(status.equals(ElementStatusEnum.Destryed))
		{
			destroy();
			isEnded = true;				
		}
		return isEnded;
	}
	
	public abstract void executeCore() throws Exception;
	
	public void notifyFinish(Object value, ConcurrentMap<String, Object> appVariables) throws Exception
	{
		notifyFinish(value, appVariables, null);
	}
	
	public void notifyFinish(Object value, ConcurrentMap<String, Object> appVariables, ElementBuilder elementBuilder) throws Exception
	{
		this.value = value;
		this.appVariables.clear();
		if(appVariables != null)
		{
			this.appVariables.putAll(appVariables);
		}
		if(elementBuilder != null)
		{
			ElementBuilder.buildDynamic(this, elementBuilder);			
		}
		iElementDao.setNotifyFinish(this);
	}
	
	public boolean notifyRecalling() throws Exception
	{
		if(status.equals(ElementStatusEnum.Running))
		{
			if(reCallingAble())
			{
				iElementDao.setReady(this);
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}
	
	protected void execute() throws Exception
	{
		iElementDao.setRunning(this);
		AsyncHelper.asyncInvoke(new IAction(){
			public void invoke(Object tParam) throws Exception {
				executeCore();
			}			
		});		
	}	
	
	protected void notifyOuput()  throws Exception
	{
		iElementDao.setNotifiedOutput(this);
	}
	
	protected void notifyInput()  throws Exception
	{
		iElementDao.setNotifiedInput(this);
	}
	
	protected void destroy() throws Exception
	{	
		iElementDao.setDestryed(this);
	}
	
	public boolean isPersistent()
	{
		return true;
	}
	
	
	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public Date getLastEditDate() {
		return lastEditDate;
	}

	public void setLastEditDate(Date lastEditDate) {
		this.lastEditDate = lastEditDate;
	}

	public Object getValue() {
		return this.value;
	}

	public ElementStatusEnum getStatus() {
		return status;
	}

	public void setStatus(ElementStatusEnum status) {
		this.status = status;
	}

	public StoreableElementDao getiElementDao() {
		return iElementDao;
	}

	public void setiElementDao(StoreableElementDao iElementDao) {
		this.iElementDao = iElementDao;
	}

	public ConcurrentMap<String, Boolean> getInNodesMap() {
		return inNodesMap;
	}

	public void setInNodesMap(ConcurrentMap<String, Boolean> inNodesMap) {
		this.inNodesMap = inNodesMap;
	}

	public ConcurrentMap<String, Boolean> getOutNodesMap() {
		return outNodesMap;
	}

	public void setOutNodesMap(ConcurrentMap<String, Boolean> outNodesMap) {
		this.outNodesMap = outNodesMap;
	}


	public ConcurrentMap<String, String> getAttributeMap() {
		return attributeMap;
	}

	public void setAttributeMap(ConcurrentMap<String, String> attributeMap) {
		this.attributeMap = attributeMap;
	}

	public UUID getWorkFlowID() {
		return workFlowID;
	}

	public void setWorkFlowID(UUID workFlowID) {
		this.workFlowID = workFlowID;
	}	
}
