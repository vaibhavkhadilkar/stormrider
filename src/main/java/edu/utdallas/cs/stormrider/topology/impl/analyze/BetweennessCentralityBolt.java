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

package edu.utdallas.cs.stormrider.topology.impl.analyze;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import edu.utdallas.cs.stormrider.topology.TopologyException;
import edu.utdallas.cs.stormrider.util.StormRiderViewConstants;
import edu.utdallas.cs.stormrider.views.Views;
import edu.utdallas.cs.stormrider.views.ViewsFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class BetweennessCentralityBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

	Views views = null ;
	
	long totalNodes = 0L ;
	
	public BetweennessCentralityBolt( String configFile ) 
	{ 
		Views views = ViewsFactory.getViews( configFile ) ;
		this.totalNodes = views.getTotalNodes() ; 
	}
	
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare( Map conf, TopologyContext context, OutputCollector collector) { }

    @Override
    public void execute( Tuple input ) 
    {
    	try
    	{
    		String node = input.getString( 0 ) ;
    		double betweenness = 0.0 ;
    		Iterator<Result> iterTable = views.getAllRows() ;
    		while( iterTable.hasNext() )
    		{
    			Result rowInNodesView = iterTable.next() ;
    			String nodeInRow = Bytes.toString( rowInNodesView.getRow() ) ;
    			String closestLandmark = views.getClosestLandmark( nodeInRow ) ;
        		Iterator<Result> iterRowsForLandmark = views.getRowsForLandmark( closestLandmark ) ;
        		while( iterRowsForLandmark.hasNext() )
        		{
        			Result rowInLandmarksView = iterRowsForLandmark.next() ;
        			String nodesInRow = Bytes.toString( rowInLandmarksView.getRow() ) ;
        			long numOfPaths = views.getNumOfPathsToLandmark( nodesInRow ) ;
        			List<String> listOfPaths = views.getListOfPathsToLandmark( nodesInRow ) ;
        			long numOfPathsWithNode = 0L ;
        			for( String path : listOfPaths )
        				if( path.contains( node ) ) numOfPathsWithNode++ ;
        			betweenness += numOfPathsWithNode * 1.0 / numOfPaths ;
        		}
    		}
    		double betC = ( betweenness * 2.0 ) / ( ( totalNodes - 1.0 ) * ( totalNodes - 2.0 ) ) ;
    		views.updateMetricValue( node, StormRiderViewConstants.colMetricBetC, betC ) ;
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in betweenness centrality bolt:: ", e ) ; }
    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { }    
}