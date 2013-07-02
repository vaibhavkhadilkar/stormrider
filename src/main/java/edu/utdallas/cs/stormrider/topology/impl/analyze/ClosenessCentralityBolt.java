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

package edu.utdallas.cs.stormrider.topology.impl.analyze;

import java.util.Iterator;
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

public class ClosenessCentralityBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

	Views views = null ;
	
	long totalNodes = 0L ;
	
	public ClosenessCentralityBolt( String configFile ) 
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
    		long sumOfPaths = 0L ;
    		String closestLandmark = views.getClosestLandmark( node ) ;
    		long initDistance = ( node == closestLandmark ) ? 0 : views.getDistToClosestLandmark( closestLandmark ) ;
    		Iterator<Result> iterRowsForLandmark = views.getRowsForLandmark( node ) ;
    		while( iterRowsForLandmark.hasNext() )
    		{
    			Result row = iterRowsForLandmark.next() ;
    			sumOfPaths += views.getDistanceToLandmark( Bytes.toString( row.getRow() ) ) + initDistance ;
    		}
    		double closeC = ( totalNodes - 1 ) / ( sumOfPaths * 1.0 ) ;
    		views.updateMetricValue( node, StormRiderViewConstants.colMetricCloseC, closeC ) ;
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in closeness centrality bolt:: ", e ) ; }
    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { }    
    
	@Override
	public Map<String, Object> getComponentConfiguration() { return null ; }
}