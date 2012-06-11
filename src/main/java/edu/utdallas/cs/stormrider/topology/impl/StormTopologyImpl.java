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

package edu.utdallas.cs.stormrider.topology.impl;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import edu.utdallas.cs.stormrider.topology.TopologyBase;
import edu.utdallas.cs.stormrider.topology.TopologyException;
import edu.utdallas.cs.stormrider.topology.impl.analyze.AnalyzeSpout;
import edu.utdallas.cs.stormrider.topology.impl.analyze.BetweennessCentralityBolt;
import edu.utdallas.cs.stormrider.topology.impl.analyze.ClosenessCentralityBolt;
import edu.utdallas.cs.stormrider.topology.impl.analyze.DegreeCentralityBolt;
import edu.utdallas.cs.stormrider.topology.impl.query.QueryBolt;
import edu.utdallas.cs.stormrider.topology.impl.query.QuerySpout;
import edu.utdallas.cs.stormrider.util.StormRiderConstants;

public class StormTopologyImpl extends TopologyBase
{
	public void submitAddTopology( String topologyName, boolean isLocalMode, int numOfWorkers, StormTopology topology )
	{
		submitTopology( topologyName, isLocalMode, numOfWorkers, topology ) ;
	}
	
	public void submitAnalyzeTopology( boolean isDistributed, boolean isLocalMode, int numOfWorkers, long interval, boolean isReified, String storeConfigFile, String viewsConfigFile )
	{
		TopologyBuilder builder = new TopologyBuilder() ;
		builder.setSpout( 1, new AnalyzeSpout( isDistributed, interval, isReified, storeConfigFile, viewsConfigFile ), 1 ) ;
		builder.setBolt( 2, new DegreeCentralityBolt( viewsConfigFile ), numOfWorkers * 4 ).shuffleGrouping( 1 ) ;
		builder.setBolt( 2, new ClosenessCentralityBolt( viewsConfigFile ), numOfWorkers * 4 ).shuffleGrouping( 1 ) ;
		builder.setBolt( 2, new BetweennessCentralityBolt( viewsConfigFile ), numOfWorkers * 4 ).shuffleGrouping( 1 ) ;
		submitTopology( StormRiderConstants.ANALYZE_TOPOLOGY_NAME + System.nanoTime(), isLocalMode, numOfWorkers, builder.createTopology() ) ;
	}
	
	public void submitQuery( boolean isDistributed, boolean isLocalMode, int numOfWorkers, long maxReports, long interval, String queryString, boolean isReified, String storeConfigFile, String hbaseConfigFile, String resultTableName )
	{
		TopologyBuilder builder = new TopologyBuilder() ;
		builder.setSpout( 1, new QuerySpout( isDistributed, maxReports, interval, queryString, isReified, storeConfigFile ), 1 ) ;
		builder.setBolt( 2, new QueryBolt( hbaseConfigFile, resultTableName ), numOfWorkers * 4 ).shuffleGrouping( 1 ) ;
		submitTopology( StormRiderConstants.QUERY_TOPOLOGY_NAME_PREFIX + System.nanoTime(), isLocalMode, numOfWorkers, builder.createTopology() ) ;
	}
	
	private void submitTopology( String topologyName, boolean isLocalMode, int numOfWorkers, StormTopology topology )
	{
		try
		{
			Map<Object, Object> conf = new HashMap<Object, Object>() ;
			conf.put( Config.TOPOLOGY_WORKERS, numOfWorkers ) ;
			if( isLocalMode ) 
			{
				conf.put( Config.TOPOLOGY_DEBUG, true ) ;
				
				LocalCluster cluster = new LocalCluster() ;
				cluster.submitTopology( topologyName, conf, topology ) ;
				Utils.sleep( 10000 ) ;
				cluster.shutdown() ;
			}
			else
				StormSubmitter.submitTopology( topologyName, conf, topology ) ;
		}
        catch( Exception e ) { throw new TopologyException( "Exception during topology submission in StormTopologyImpl:: ", e ) ; }		
	}
}