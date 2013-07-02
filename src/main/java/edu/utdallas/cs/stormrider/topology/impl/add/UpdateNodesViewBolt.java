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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

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
	
	public UpdateNodesViewBolt( boolean isReified, String storeConfigFile, String viewsConfigFile ) 
	{ 
		store = StoreFactory.getJenaHBaseStore( storeConfigFile, isReified ) ;
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
    		if( input.getString( 0 ).equals( "nv" ) )
    		{
    			String node = input.getString( 1 ) ;
    			if( !views.getIsLandmark( node ) )
    			{
    				String adjList = store.getAdjacencyList( node, views.getLinkNameAsURI() ) ;
    				views.updateAdjacencyListValue( node, adjList ) ;
    				ClosestLandmark closestLandmark = BFS( node, adjList ) ;
    				StringBuilder sb = new StringBuilder() ; sb.append( closestLandmark.getDistance() ) ;
    				views.updateIsLandmarkValue( node, "N" ) ;
    				views.updateDistToClosestLandmarkValue( node, sb.toString() ) ;
    				views.updateClosestLandmarkValue( node, closestLandmark.getLandmark() ) ;
    				sb = null ; adjList = null ;
    			}
    		}
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in update nodes view bolt:: ", e ) ; }
    }

	private ClosestLandmark BFS( String node, String adjList )
    {
		boolean isClosestLandmarkFound = false ;
    	ClosestLandmark closestLandmark = new ClosestLandmark() ;
    	
    	//Number of iterations to perform in BFS
    	long distance = 1L ;
		
    	//Neighbors of nodes that will be processed in this iteration
		Set<String> setNodes = new LinkedHashSet<String>() ;
		setNodes.addAll( Arrays.asList( adjList.split( "~" ) ) ) ;
		
		while( distance < 15 )
    	{
    		for( String neighbor : setNodes )
    		{
    			if( views.getIsLandmark( neighbor ) )
    			{
    				closestLandmark.setNode( node ) ;
    				closestLandmark.setDistance( distance ) ;
    				closestLandmark.setLandmark( neighbor ) ;
    				isClosestLandmarkFound = true ; break ;
    			}
    			else
    				setNodes.addAll( Arrays.asList( store.getAdjacencyList( neighbor, views.getLinkNameAsURI() ).split( "~" ) ) ) ;
    			setNodes.remove( neighbor ) ;
    		}
    		if( isClosestLandmarkFound ) break ;
    		distance++ ;
    	}
    	return closestLandmark ;
    }
    
    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { }    
    
	@Override
	public Map<String, Object> getComponentConfiguration() { return null ; }
}