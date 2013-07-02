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

package edu.utdallas.cs.stormrider.examples.twitter;

import edu.utdallas.cs.stormrider.topology.Topology;
import edu.utdallas.cs.stormrider.topology.TopologyFactory;
import edu.utdallas.cs.stormrider.util.TwitterConstants;

public class TwitterQueryOnlyTopology 
{
	public static void main( String[] args )
	{
		//Create a topology object
		Topology topology = TopologyFactory.getStormTopology() ;
		
		//Submit various queries to the query topology
		//A simple query that tracks users in a particular location
		String queryString =
		" PREFIX twitter: <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" SELECT ?x ?y " +
		" WHERE { ?x twitter:Screen_Name ?y } " ;
		topology.submitQuery( TwitterConstants.IS_LOCAL_MODE, TwitterConstants.NUM_OF_WORKERS, 
				              TwitterConstants.MAX_REPORTS, TwitterConstants.INTERVAL, queryString, true,
				              TwitterConstants.HBASE_MODEL_CONFIG_FILE, TwitterConstants.IRI, TwitterConstants.HBASE_CONFIG_FILE, 
				              "query-users-and-names" ) ;		
	}
}