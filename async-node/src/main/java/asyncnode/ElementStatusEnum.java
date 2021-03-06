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

public enum ElementStatusEnum {
	New(0),
	Pending(1),
	Ready(2),
	Running(3),
	NotifyFinish(4),
	NotifiedOutput(5),
	NotifiedInput(6),
	Destryable(7),
	Destryed(8);
	
	private int value;
	private ElementStatusEnum(int value)
	{
		this.value = value;
	}	
	
	public int getValue()
	{
		return value;
	}
}