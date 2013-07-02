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

package edu.utdallas.cs.stormrider.topology;

import backtype.storm.generated.StormTopology;

public interface Topology 
{
	public void submitAddTopology( String topologyName, boolean isLocalMode, int numOfWorkers, StormTopology topology ) ;
	
	public void submitAnalyzeTopology( boolean isLocalMode, int numOfWorkers, long interval, boolean isReified, String storeConfigFile, String viewsConfigFile ) ;
	
	public void submitQuery( boolean isLocalMode, int numOfWorkers, long maxReports, long interval, String queryString, boolean isReified, 
							 String storeConfigFile, String iri, String hbaseConfigFile, String resultTableName ) ;
}
