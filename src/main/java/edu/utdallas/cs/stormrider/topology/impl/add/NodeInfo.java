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

import java.util.ArrayList;
import java.util.List;

public class NodeInfo 
{
	private String node = null ;
	
	private long distance = 0L ;
	
	private long numOfPaths = 0L ;
	
	private List<String> listOfPaths = null ;
	
	public NodeInfo() { listOfPaths = new ArrayList<String>() ; }
	
	public void setNode( String node ) { this.node = node ; }
	
	public String getNode() { return node ; }
	
	public void setDistance( long distance ) { this.distance = distance ; }
	
	public long getDistance() { return distance ; }
	
	public void setNumOfPaths( long numOfPaths ) { this.numOfPaths = numOfPaths ; }
	
	public long getNumOfPaths() { return numOfPaths ; }
	
	public void addPath( String path ) { listOfPaths.add( path ) ; }
	
	public List<String> getAllPaths() { return listOfPaths ; }
	
	public String getPath( int pos ) { return listOfPaths.get( pos ) ; }
}
