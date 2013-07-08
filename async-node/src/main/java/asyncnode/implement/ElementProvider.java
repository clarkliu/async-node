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

package asyncnode.implement;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.google.inject.Singleton;

import asyncnode.ElementBase;

@Singleton
public class ElementProvider {
	public ElementBase getInstance(String elementName) throws Exception {
		try
		{
			Class<ElementBase> elementBase = (Class<ElementBase>)Class.forName(elementName, false, this.getClass().getClassLoader());
			Constructor<ElementBase> elementProvider = elementBase.getDeclaredConstructor(null);
			elementProvider.setAccessible(true);
			return elementProvider.newInstance();
		}
		catch(Exception ex)
		{
			throw ex;
		}
	}
}
