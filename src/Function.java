/********************************************************************************
   Copyright 2011-2012 Leonidas Fegaras, University of Texas at Arlington

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   File: Function.java
   An anonymous function must be an instance of a public class to make sure the
      Java classLoader can find it during dynamic loading
   Programmer: Leonidas Fegaras, UTA
   Date: 10/14/10 - 07/12/12

********************************************************************************/

package hadoop.mrql;


// An anonymous function from MRData to MRData
abstract public class Function {
    abstract public MRData eval ( final MRData v );
}
