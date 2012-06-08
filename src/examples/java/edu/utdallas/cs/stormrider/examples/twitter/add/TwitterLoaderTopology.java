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

package edu.utdallas.cs.stormrider.examples.twitter.add;

import edu.utdallas.cs.stormrider.topology.impl.add.TripleLoaderBolt;
import edu.utdallas.cs.stormrider.util.TwitterConstants;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class TwitterLoaderTopology 
{
	public static StormTopology constructAddTopology()
	{
		TopologyBuilder builder = new TopologyBuilder() ;
		builder.setSpout( 1, new TwitterLoaderSpout( TwitterConstants.HBASE_VIEW_CONFIG_FILE ), 1 ) ;
		builder.setBolt( 2, new TripleLoaderBolt( TwitterConstants.HBASE_MODEL_CONFIG_FILE ), TwitterConstants.NUM_OF_TASKS ).shuffleGrouping( 1 ) ;
		return builder.createTopology() ;
	}
}