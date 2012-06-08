/*
 * Copyright © 2012 The University of Texas at Dallas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.utdallas.cs.stormrider.topology;

public class TopologyException extends RuntimeException
{
	private static final long serialVersionUID = -6015899413647839006L;

	public TopologyException() 								{ super() ; }
    public TopologyException( Throwable cause ) 			{ super( cause ) ; }
    public TopologyException( String msg ) 					{ super( msg ) ; }
    public TopologyException( String msg, Throwable cause ) { super( msg, cause ) ; }
}