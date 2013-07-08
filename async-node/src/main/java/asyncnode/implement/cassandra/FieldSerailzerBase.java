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

package asyncnode.implement.cassandra;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import asyncnode.ElementStatusEnum;

public class FieldSerailzerBase
{
	public FieldSerailzerBase()
	{
		
	}
	
	private static final String dateFormatString = "yyyy-MM-dd HH:mm:ss.SSS";
	protected Class fieldClass = null;
	public static byte[] emptyByteArray = new byte[0];
	public void setFieldClass(Class fieldClass)
	{
		this.fieldClass = fieldClass;
	}
	
	public static byte[] getStoreValue(Class fieldClass, Object fieldValue) throws Exception	
	{		
		if(fieldValue == null)
		{
			return emptyByteArray;
		}
		else
		{
			if(fieldClass == null)
			{
				throw new Exception("class can not be null!");
			}
			
			byte[] btArray = null;
			if(fieldClass.equals(Date.class))
			{
				btArray = new SimpleDateFormat(dateFormatString).format((Date)fieldValue).getBytes();				
			}
			else if(fieldClass.equals(String.class))
			{
				btArray = ((String)fieldValue).getBytes();				
			}
			else if(fieldClass.equals(byte.class) || fieldClass.equals(Byte.class))
			{
				btArray = new byte[]{((Byte)fieldValue).byteValue()};
			}
			else if(fieldClass.equals(short.class) || fieldClass.equals(Short.class))
			{
				btArray = fieldValue.toString().getBytes();
			}
			else if(fieldClass.equals(int.class) || fieldClass.equals(Integer.class))
			{
				btArray = Integer.toHexString(((Integer)fieldValue).intValue()).getBytes();
			}
			else if(fieldClass.equals(long.class) || fieldClass.equals(Long.class))
			{
				btArray = Long.toHexString(((Long)fieldValue).longValue()).getBytes();
			}
			else if(fieldClass.equals(float.class) || fieldClass.equals(Float.class))
			{
				btArray = Float.toString(((Float)fieldValue).floatValue()).getBytes();
			}
			else if(fieldClass.equals(double.class) || fieldClass.equals(Double.class))
			{		
				btArray = Double.toString(((Double)fieldValue).doubleValue()).getBytes();
			}
			else if(fieldClass.equals(BigDecimal.class))
			{
				btArray = fieldValue.toString().getBytes();
			}
			else if(fieldClass.equals(char.class))
			{
				btArray = fieldValue.toString().getBytes();
			}
			else if(fieldClass.isEnum())
			{
				btArray = fieldValue.toString().getBytes();
			}
			else if(fieldClass.equals(boolean.class) || fieldClass.equals(Boolean.class))
			{
				Boolean boolValue = (Boolean)fieldValue;
				byte btValue = 0;
				if(boolValue.booleanValue())
				{
					btValue = 1;
				}
				btArray = new byte[]{ btValue };
			}
			else if(fieldClass.equals(UUID.class))
			{				
				btArray = fieldValue.toString().getBytes();
			}
			else
			{
				ObjectMapper objectMapper = new ObjectMapper();
				btArray = objectMapper.writeValueAsBytes(fieldValue);
			}
			return btArray;
		}
	}
	
	public byte[] getStoreValue(Object fieldValue) throws Exception {
		return getStoreValue(fieldClass, fieldValue);
	}
	
	public static Object getRuntimeValue(Class fieldClass, byte[] fieldValue) throws Exception
	{	
		if(fieldValue == null || fieldValue.length == 0)
		{
			return null;
		}
		else
		{
			if(fieldClass == null)
			{
				throw new Exception("class can not be null!");
			}
			
			Object result = null;
			if(fieldClass.equals(Date.class))
			{
				result = new SimpleDateFormat(dateFormatString).parse(new String(fieldValue));		
			}
			else if(fieldClass.equals(String.class))
			{
				result = new String(fieldValue);			
			}
			else if(fieldClass.equals(byte.class) || fieldClass.equals(Byte.class))
			{
				result = fieldValue[0];
			}
			else if(fieldClass.equals(short.class) || fieldClass.equals(Short.class))
			{
				result = Short.parseShort(new String(fieldValue));
			}
			else if(fieldClass.equals(int.class) || fieldClass.equals(Integer.class))
			{			
				result = Integer.parseInt(new String(fieldValue), 16);
			}
			else if(fieldClass.equals(long.class) || fieldClass.equals(Long.class))
			{
				result = Long.parseLong(new String(fieldValue), 16);
			}
			else if(fieldClass.equals(float.class) || fieldClass.equals(Float.class))
			{
				result = Float.parseFloat(new String(fieldValue));
			}
			else if(fieldClass.equals(double.class) || fieldClass.equals(Double.class))
			{
				result = Double.parseDouble(new String(fieldValue));
			}
			else if(fieldClass.equals(BigDecimal.class))
			{
				result = new BigDecimal(new String(fieldValue));
			}
			else if(fieldClass.equals(char.class))
			{
				result = new String(fieldValue).charAt(0);
			}
			else if(fieldClass.equals(boolean.class) || fieldClass.equals(Boolean.class))
			{
				byte btValue = fieldValue[0];
				if(btValue == 0)
				{
					result = false;
				}
				else
				{
					result = true;
				}
			}
			else if(fieldClass.isEnum())
			{
				result = Enum.valueOf(fieldClass, new String(fieldValue));
			}
			else if(fieldClass.equals(UUID.class))
			{				
				result = UUID.fromString(new String(fieldValue));
			}
			else
			{
				ObjectMapper objectMapper = new ObjectMapper();
				result = objectMapper.readValue(fieldValue, fieldClass);	
			}
			return result;
		}
	}
	
	public Object getRuntimeValue(byte[] fieldValue) throws Exception {
		return getRuntimeValue(fieldClass, fieldValue);
	}
}
