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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.emory.mathcs.backport.java.util.Arrays;
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

public class UpdateLandmarksViewBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

	private Store store = null ;
	
	private Views views = null ;
	
	private List<String> nonLandmarksList = new ArrayList<String>() ;
	
	public UpdateLandmarksViewBolt( boolean isReified, String storeConfigFile, String viewsConfigFile ) 
	{ 
		store = StoreFactory.getJenaHBaseStore( storeConfigFile, isReified ) ;
		views = ViewsFactory.getViews( viewsConfigFile ) ;
		nonLandmarksList = views.getNonLandmarksList() ;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare( Map conf, TopologyContext context, OutputCollector collector) { }

	@SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute( Tuple input ) 
    {
    	try
    	{
    		List<List> landmarksLists = (List) input.getValue( 0 ) ;
            for( List landmarkList : landmarksLists )
            {
            	String landmark = (String) landmarkList.get( 0 ) ;
            	LandmarkInfo landmarkInfo = SSSP( landmark ) ;
            	for( String node : nonLandmarksList )
            	{
            		NodeInfo nodeInfo = landmarkInfo.getNodeInfo( node ) ;
            		StringBuilder sb = new StringBuilder() ; sb.append( landmark ) ; sb.append( "&" ) ; sb.append( node ) ;
            		StringBuilder sbDist = new StringBuilder() ; sbDist.append( nodeInfo.getDistance() ) ;
            		StringBuilder sbNumOfPaths = new StringBuilder() ; sbNumOfPaths.append( nodeInfo.getNumOfPaths() ) ;
            		views.updateDistanceValue( sb.toString(), sbDist.toString() ) ;
            		views.updateNumOfPathsValue( sb.toString(), sbNumOfPaths.toString() ) ;
            		for( int i = 0 ; i < nodeInfo.getNumOfPaths() ; i++ )
            			views.updatePathsValue( sb.toString(), nodeInfo.getPath( i ) ) ;
            		sb = null ; sbDist = null ; sbNumOfPaths = null ;
            	}
				String adjList = store.getAdjacencyList( landmark, views.getLinkNameAsURI() ) ;
				views.updateAdjacencyListValue( landmark, adjList ) ;
				views.updateIsLandmarkValue( landmark, "Y" ) ;
				views.updateDistToClosestLandmarkValue( landmark, "0" ) ;
				views.updateClosestLandmarkValue( landmark, landmark ) ;
            }
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in update nodes view bolt:: ", e ) ; }
    }

    @SuppressWarnings("unchecked")
	private LandmarkInfo SSSP( String landmark )
    {
    	LandmarkInfo landmarkInfo = new LandmarkInfo() ;
    	landmarkInfo.setLandmark( landmark ) ;
    	
    	//Maximum depth to explore
    	long depth = 1L ;

    	//Get adjacency list
    	Map<String, List<String>> mapCurrIterNodeToAdjList = new LinkedHashMap<String, List<String>>() ;
    	mapCurrIterNodeToAdjList.put( landmark, Arrays.asList( store.getAdjacencyList( landmark, views.getLinkNameAsURI() ).split( "~" ) ) ) ;
    	Map<String, List<String>> mapNextIterNodeToAdjList = new LinkedHashMap<String, List<String>>() ;
    	
    	while( depth < 15 )
    	{
    		Iterator<String> iterKeys = mapCurrIterNodeToAdjList.keySet().iterator() ;
    		while( iterKeys.hasNext() )
    		{
    			String prevIterNode = iterKeys.next() ;
    			List<String> currIterNodeList = mapCurrIterNodeToAdjList.get( prevIterNode ) ;
    			for( String node : currIterNodeList )
    			{
        			NodeInfo nInfo = landmarkInfo.getNodeInfo( node ) ;
        			if( nInfo == null )
        			{
        				nInfo = new NodeInfo() ;
        				nInfo.setNode( node ) ;
        				nInfo.setDistance( depth ) ;
        				if( depth == 1 )
        				{
    	        			StringBuilder sb = new StringBuilder() ; sb.append( landmark ) ; sb.append( "~" ) ; sb.append( node ) ;
    	        			nInfo.setNumOfPaths( 1 ) ;
    	        			nInfo.addPath( sb.toString() ) ;
    	        			sb = null ;	        					
        				}
        				else
        				{
	        				NodeInfo pInfo = landmarkInfo.getNodeInfo( prevIterNode ) ;
	        				nInfo.setNumOfPaths( pInfo.getNumOfPaths() ) ;
	        				addNewPath( node, pInfo, nInfo ) ;
        				}
        			}
        			else if( nInfo.getDistance() == depth )
        			{
        				NodeInfo pInfo = landmarkInfo.getNodeInfo( prevIterNode ) ;
        				nInfo.setNumOfPaths( nInfo.getNumOfPaths() + pInfo.getNumOfPaths() ) ;
        				addNewPath( node, pInfo, nInfo ) ;
        			}
        			landmarkInfo.addNodeInfo( node, nInfo ) ;
	    			if( depth < 15 ) mapNextIterNodeToAdjList.put( node, Arrays.asList( store.getAdjacencyList( node, views.getLinkNameAsURI() ).split( "~" ) ) ) ;
    			}
    		}
    		mapCurrIterNodeToAdjList.clear() ; mapCurrIterNodeToAdjList.putAll( mapNextIterNodeToAdjList ) ;
    		depth++ ;
    	}
    	return landmarkInfo ;
    }
    
    private void addNewPath( String node, NodeInfo pInfo, NodeInfo nInfo )
    {
		for( int i = 0 ; i < pInfo.getNumOfPaths() ; i++ )
		{
			StringBuilder sb = new StringBuilder() ;
			sb.append( pInfo.getPath( i ) ) ; sb.append( "~" ) ; sb.append( node ) ;
			nInfo.addPath( sb.toString() ) ;
			sb = null ;
		}
    }
    
    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { }    
}