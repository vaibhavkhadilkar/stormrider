/*
 * Copyright © 2012-2013 The University of Texas at Dallas
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

import java.util.HashMap;
import java.util.Map;

public class LandmarkInfo 
{
	private String landmark = null ;
	
	private Map<String, NodeInfo> mapNodeToInfo = null ;
	
	public LandmarkInfo() { mapNodeToInfo = new HashMap<String, NodeInfo>() ; }
	
	public void setLandmark( String landmark ) { this.landmark = landmark ; }
	
	public String getLandmark() { return landmark ; }
	
	public void addNodeInfo( String node, NodeInfo nInfo ) { mapNodeToInfo.put( node, nInfo ) ; }
	
	public NodeInfo getNodeInfo( String node ) { return mapNodeToInfo.get( node ) ; }
}
