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

package edu.utdallas.cs.stormrider.topology.impl;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
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
import edu.utdallas.cs.stormrider.util.TwitterConstants;

public class StormTopologyImpl extends TopologyBase
{
	public void submitAddTopology( String topologyName, boolean isLocalMode, int numOfWorkers, StormTopology topology )
	{
		submitTopology( topologyName, isLocalMode, numOfWorkers, topology ) ;
	}
	
	public void submitAnalyzeTopology( boolean isLocalMode, int numOfWorkers, long interval, boolean isReified, String storeConfigFile, String viewsConfigFile )
	{
		TopologyBuilder builder = new TopologyBuilder() ;
		builder.setSpout( "analyze", new AnalyzeSpout( interval, isReified, storeConfigFile, viewsConfigFile ), TwitterConstants.PARALLELISM_HINT ) ;
		builder.setBolt( "deg-cen", new DegreeCentralityBolt( viewsConfigFile ), TwitterConstants.PARALLELISM_HINT ).shuffleGrouping( "analyze" ) ;
		builder.setBolt( "close-cen", new ClosenessCentralityBolt( viewsConfigFile ), TwitterConstants.PARALLELISM_HINT ).shuffleGrouping( "analyze" ) ;
		builder.setBolt( "bet-cen", new BetweennessCentralityBolt( viewsConfigFile ), TwitterConstants.PARALLELISM_HINT ).shuffleGrouping( "analyze" ) ;
		submitTopology( StormRiderConstants.ANALYZE_TOPOLOGY_NAME + System.nanoTime(), isLocalMode, numOfWorkers, builder.createTopology() ) ;
	}
	
	public void submitQuery( boolean isLocalMode, int numOfWorkers, long maxReports, long interval, String queryString, boolean isReified, 
							 String storeConfigFile, String iri, String hbaseConfigFile, String resultTableName )
	{
		TopologyBuilder builder = new TopologyBuilder() ;
		builder.setSpout( "query", new QuerySpout( maxReports, interval, queryString, isReified, storeConfigFile, iri ), TwitterConstants.PARALLELISM_HINT ) ;
		builder.setBolt( "results", new QueryBolt( hbaseConfigFile, resultTableName, queryString ), TwitterConstants.PARALLELISM_HINT ).shuffleGrouping( "query" ) ;
		submitTopology( StormRiderConstants.QUERY_TOPOLOGY_NAME_PREFIX + System.nanoTime(), isLocalMode, numOfWorkers, builder.createTopology() ) ;
	}
	
	private void submitTopology( String topologyName, boolean isLocalMode, int numOfWorkers, StormTopology topology )
	{
		try
		{
			final Map<Object, Object> conf = new HashMap<Object, Object>() ;
			final String topName = topologyName ;
			final StormTopology top = topology ;
			conf.put( Config.TOPOLOGY_WORKERS, numOfWorkers ) ;
			conf.put( Config.TOPOLOGY_MAX_TASK_PARALLELISM, TwitterConstants.MAX_TASK_PARALLELISM ) ;
			conf.put( Config.TOPOLOGY_TASKS, TwitterConstants.NUM_OF_TASKS ) ;
			conf.put( Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE, TwitterConstants.WORKER_SHARED_THREAD_POOL_SIZE ) ;
			conf.put( Config.SUPERVISOR_SLOTS_PORTS, "6700" ) ;
			
			if( isLocalMode ) 
			{
				conf.put( Config.TOPOLOGY_DEBUG, true ) ;	

				MkClusterParam clusterParam = new MkClusterParam() ;
				clusterParam.setSupervisors( 1 ) ;
				clusterParam.setPortsPerSupervisor( 1 ) ;
				clusterParam.setDaemonConf( conf ) ;
				
				Testing.withLocalCluster( clusterParam, 
				new TestJob() 
				{
					@Override
					public void run( ILocalCluster cluster ) throws Exception 
					{
						cluster.submitTopology( topName, conf, top ) ;
						Utils.sleep( 1000000 ) ;
						cluster.shutdown() ;
					}
				} ) ;
//				LocalCluster cluster = new LocalCluster() ;
//				cluster.submitTopology( topologyName, conf, topology ) ;
			}
			else
				StormSubmitter.submitTopology( topologyName, conf, topology ) ;
		}
        catch( Exception e ) { throw new TopologyException( "Exception during topology submission in StormTopologyImpl:: ", e ) ; }		
	}
}