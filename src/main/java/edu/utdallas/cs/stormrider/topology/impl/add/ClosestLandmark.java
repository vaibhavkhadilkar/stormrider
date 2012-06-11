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

package edu.utdallas.cs.stormrider.topology.impl.add;

public class ClosestLandmark 
{
	private String node = null ;
	
	private long distance = 0L ; 
	
	private String landmark = null ;
	
	public void setNode( String node ) { this.node = node ; }
	
	public String getNode() { return node ; }
	
	public void setDistance( long distance ) { this.distance = distance ; }
	
	public long getDistance() { return distance ; }
	
	public void setLandmark( String landmark ) { this.landmark = landmark ; }
	
	public String getLandmark() { return landmark ; }
}
