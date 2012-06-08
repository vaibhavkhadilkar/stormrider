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

import java.util.Map;

import edu.utdallas.cs.stormrider.store.Store;
import edu.utdallas.cs.stormrider.store.StoreFactory;
import edu.utdallas.cs.stormrider.topology.TopologyException;
import edu.utdallas.cs.stormrider.views.Views;
import edu.utdallas.cs.stormrider.views.ViewsFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class UpdateNodesViewBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

	private Store store = null ;
	
	private Views views = null ;
	
	public UpdateNodesViewBolt( String storeConfigFile, String viewsConfigFile ) 
	{ 
		store = StoreFactory.getJenaHBaseStore( storeConfigFile ) ;
		views = ViewsFactory.getViews( viewsConfigFile ) ;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare( Map conf, TopologyContext context, OutputCollector collector) { }

    @Override
    public void execute( Tuple input ) 
    {
    	try
    	{
    		if( input.getString( 0 ).equals( "node-nv" ) )
    		{
    			String node = input.getString( 1 ) ;
    			String adjList = store.getAdjacencyList( views.getLinkNameAsURI() ) ;
    			views.updateAdjacencyListValue( node, adjList ) ;
    			if( !views.getIsLandmark( node ) )
    			{
    				String[] closestLandmark = BFS( node, adjList ) ;
    				views.updateIsLandmarkValue( node, "N" ) ;
    				views.updateDistToClosestLandmarkValue( node, closestLandmark[0] ) ;
    				views.updateClosestLandmarkValue( node, closestLandmark[1] ) ;
    			}
    		}
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in update nodes view bolt:: ", e ) ; }
    }

    private String[] BFS( String node, String adjList )
    {
    	String[] closestLandmark = new String[2] ;
    	return closestLandmark ;
    }
    
    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { }    
}