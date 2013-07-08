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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InstanceFactoryHelper
{
	private static ConcurrentMap<Class, Object> instanceMap = new ConcurrentHashMap<Class, Object>();
	public static <T> T getSingleInstance(Class<T> classType) throws Exception
	{
		T instance = null;
		if(instanceMap.containsKey(classType))
		{
			instance = (T)instanceMap.get(classType);
		}
		else
		{
			instance = classType.newInstance();
			instanceMap.put(classType, instance);
		}
		return instance;
	}
}