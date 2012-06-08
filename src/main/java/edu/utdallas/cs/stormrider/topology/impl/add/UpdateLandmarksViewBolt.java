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
import java.util.List;
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

public class UpdateLandmarksViewBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

	private Store store = null ;
	
	private Views views = null ;
	
	private List<String> nonLandmarksList = new ArrayList<String>() ;
	
	public UpdateLandmarksViewBolt( String storeConfigFile, String viewsConfigFile ) 
	{ 
		store = StoreFactory.getJenaHBaseStore( storeConfigFile ) ;
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
            	for( String node : nonLandmarksList )
            	{
            		
            	}
            }
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in update nodes view bolt:: ", e ) ; }
    }

    private String[] SSSP( String landmark )
    {
    	String[] closestLandmark = new String[2] ;
    	return closestLandmark ;
    }
    
    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { }    
}