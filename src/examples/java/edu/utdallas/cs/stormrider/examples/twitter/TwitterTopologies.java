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

package edu.utdallas.cs.stormrider.examples.twitter;

import edu.utdallas.cs.stormrider.examples.twitter.add.TwitterLoaderTopology;
import edu.utdallas.cs.stormrider.topology.Topology;
import edu.utdallas.cs.stormrider.topology.TopologyFactory;
import edu.utdallas.cs.stormrider.util.TwitterConstants;

public class TwitterTopologies 
{
	public void main( String[] args )
	{
		//Create a topology object
		Topology topology = TopologyFactory.getStormTopology() ;
		
		//Create and Submit Add Topology
		topology.submitAddTopology( TwitterConstants.STORAGE_TOPOLOGY_NAME, false, TwitterConstants.NUM_OF_WORKERS, TwitterLoaderTopology.constructAddTopology() ) ;
		
		//Submit various queries to the query topology
		//A simple query that tracks users in a particular location
		String queryString =
		" PREFIX twitter: <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" SELECT ?x " +
		" WHERE { ?x twitter:Location \"Richardson, TX\" } " ;
		topology.submitQuery( TwitterConstants.IS_DISTRIBUTED, TwitterConstants.IS_LOCAL_MODE, TwitterConstants.NUM_OF_WORKERS, 
				              TwitterConstants.MAX_REPORTS, TwitterConstants.INTERVAL, queryString, true,
				              TwitterConstants.HBASE_MODEL_CONFIG_FILE, TwitterConstants.HBASE_CONFIG_FILE, "query-simple1-location" ) ;
		
		//A query that tracks the neighborhood of a user
		queryString = 
		" PREFIX ex: <http://www.example.org/people#> " +
		" PREFIX twitter: <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" SELECT ?x " +
		" WHERE { <ex:u-15000001> gleen:onPath(\"([twitter:Has_Friend]/[twitter:Has_Friend])\" ?x) } " ;
		topology.submitQuery( TwitterConstants.IS_DISTRIBUTED, TwitterConstants.IS_LOCAL_MODE, TwitterConstants.NUM_OF_WORKERS, 
	              			  TwitterConstants.MAX_REPORTS, TwitterConstants.INTERVAL, queryString, true,
	              			  TwitterConstants.HBASE_MODEL_CONFIG_FILE, TwitterConstants.HBASE_CONFIG_FILE, "query-complex1-neighborhood" ) ;

		//A user that queries an older version of the graph
		queryString = 
		" PREFIX ex: <http://www.example.org/people#> " +
		" PREFIX twitter: <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
		" SELECT ?z " +
		" WHERE " +
		" { " +
		"		?x twitter:Timestamp ?y . " +
		"		?x rdf:subject <ex:u-15000001> . " +
		"		?x rdf:predicate <twitter:Has_Friend> . " +
		"		?x rdf:object ?z . " +
		"		FILTER( ?y <= \"2012-06-05T21:00:00\"^^xsd:dateTime ) " +
		" } " ;
		topology.submitQuery( TwitterConstants.IS_DISTRIBUTED, TwitterConstants.IS_LOCAL_MODE, TwitterConstants.NUM_OF_WORKERS, 
	              			  TwitterConstants.MAX_REPORTS, TwitterConstants.INTERVAL, queryString, true,
	              			  TwitterConstants.HBASE_MODEL_CONFIG_FILE, TwitterConstants.HBASE_CONFIG_FILE, "query-complex2-versioning" ) ;

		//Queries that monitor various keywords
		queryString = 
		" PREFIX ex: <http://www.example.org/people#> " +
		" PREFIX twitter: <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" SELECT ?x ?y " +
		" WHERE " +
		" { " +
		" 		?x tw:Has_Tweet ?y " +
		"		?y tw:Tweet_Text ?z " +
		"		FILTER regex(?z, \"bombing\", \"i\") " +
		" } " ;
		topology.submitQuery( TwitterConstants.IS_DISTRIBUTED, TwitterConstants.IS_LOCAL_MODE, TwitterConstants.NUM_OF_WORKERS, 
	              			  TwitterConstants.MAX_REPORTS, TwitterConstants.INTERVAL, queryString, true,
	              			  TwitterConstants.HBASE_MODEL_CONFIG_FILE, TwitterConstants.HBASE_CONFIG_FILE, "query-complex3-monitoring1" ) ;

		queryString = 
		" PREFIX ex: <http://www.example.org/people#> " +
		" PREFIX twitter: <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" SELECT ?x ?y " +
		" WHERE " +
		" { " +
		" 		?x tw:Has_Tweet ?y " +
		"		?y tw:Tweet_Text ?z " +
		"		FILTER regex(?z, \".*(bombing|explosion).*america.*\", \"i\") " +
		" } " ;
		topology.submitQuery( TwitterConstants.IS_DISTRIBUTED, TwitterConstants.IS_LOCAL_MODE, TwitterConstants.NUM_OF_WORKERS, 
		             		  TwitterConstants.MAX_REPORTS, TwitterConstants.INTERVAL, queryString, true,
		             		  TwitterConstants.HBASE_MODEL_CONFIG_FILE, TwitterConstants.HBASE_CONFIG_FILE, "query-complex3-monitoring2" ) ;

		queryString = 
		" PREFIX ex: <http://www.example.org/people#> " +
		" PREFIX twitter: <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" SELECT ?x ?y " +
		" WHERE " +
		" { " +
		" 		?x tw:Has_Tweet ?y " +
		"		?y tw:Tweet_Text ?z " +
		"		?y tw:Tweet_Place \"Richardson,TX\"^^xsd:string " +
		"		FILTER regex(?z, \".*(bombing|explosion).*america.*\", \"i\") " +
		" } " ;
		topology.submitQuery( TwitterConstants.IS_DISTRIBUTED, TwitterConstants.IS_LOCAL_MODE, TwitterConstants.NUM_OF_WORKERS, 
		             		  TwitterConstants.MAX_REPORTS, TwitterConstants.INTERVAL, queryString, true,
		             		  TwitterConstants.HBASE_MODEL_CONFIG_FILE, TwitterConstants.HBASE_CONFIG_FILE, "query-complex3-monitoring3" ) ;
		
		//Start the analyze topology
		topology.submitAnalyzeTopology( TwitterConstants.IS_DISTRIBUTED, TwitterConstants.IS_LOCAL_MODE, TwitterConstants.NUM_OF_WORKERS, 
										TwitterConstants.INTERVAL, true, TwitterConstants.HBASE_MODEL_CONFIG_FILE, TwitterConstants.HBASE_VIEW_CONFIG_FILE ) ;
	}
}