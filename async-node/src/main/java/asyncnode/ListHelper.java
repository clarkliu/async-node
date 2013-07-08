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

import java.util.List;

import asyncnode.core.IFunc;

public class ListHelper {
	public static <T> T first(List<T> targetList, IFunc<T, Boolean> isOk) throws Exception
	{
		T result = null;
		for(T item : targetList)
		{
			if(isOk.getValue(item))
			{
				return item;
			}
		}
		throw new Exception("can't find item!");
	}
}
