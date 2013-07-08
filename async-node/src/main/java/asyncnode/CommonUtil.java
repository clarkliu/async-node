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
import java.util.List;

import asyncnode.core.IFunc;

public class CommonUtil {
	public static <I, T> List<T> getPagedList(List<I> list, 
			int pageSize,
			IFunc<List<I>, List<T>> func) throws Exception
	{
		if(list == null)
		{
			return null;			
		}
		
		List<T> resultList = new ArrayList<T>();
		List<I> itemList = new ArrayList<I>();
		for(I item : list)
		{
			itemList.add(item);
			if(itemList.size() == pageSize)
			{
				resultList.addAll(func.getValue(itemList));
				itemList = new ArrayList();				
			}
		}	
	
		if(itemList.size() > 0)
		{
			resultList.addAll(func.getValue(itemList));
		}		
		
		return resultList;
	}
}
