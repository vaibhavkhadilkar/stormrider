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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import edu.utdallas.cs.stormrider.topology.TopologyException;
import edu.utdallas.cs.stormrider.views.Views;
import edu.utdallas.cs.stormrider.views.ViewsFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RankUsersBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

    /** An output collector used to emit tuples from the Twitter stream **/
    private OutputCollector collector ;

    private List<List<Object>> rankings = new ArrayList<List<Object>>() ;
    
    private Long k = null ;
    
    public RankUsersBolt( String viewConfigFile )
    {
    	Views views = ViewsFactory.getViews( viewConfigFile ) ;
    	this.k = views.getTotalNodes() / 100 ;
    }
    
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare( Map conf, TopologyContext context, OutputCollector collector) { this.collector = collector ; }

	private int compareLists( List<Object> one, List<Object> two ) 
	{
        long valueOne = (Long) one.get(1) ;
        long valueTwo = (Long) two.get(1) ;
        long delta = valueTwo - valueOne ;
        if( delta > 0 ) return 1 ; 
        else if ( delta < 0 ) return -1 ;
        else return 0 ;
    }
	
	private Integer find( Object tag ) 
	{
        for( int i = 0 ; i < rankings.size() ; i++ ) 
        {
            Object cur = rankings.get( i ).get( 0 ) ;
            if( cur.equals( tag ) ) return i ;
        }
        return null ;
    }
	
    @Override
    public void execute( Tuple input ) 
    {
    	try
    	{
            Object node = input.getValue( 0 ) ;
            Integer nodePos = find( node ) ;
            
            if( null != nodePos ) rankings.set( nodePos, input.getValues() ) ;
            else rankings.add( input.getValues() ) ;
            
            Collections.sort( rankings, new Comparator<List<Object>>() 
            {
                public int compare( List<Object> o1, List<Object> o2 ) { return compareLists( o1, o2 ) ; }
            });
            
            if ( rankings.size() > k ) rankings.remove( k ) ;
            
            collector.emit( new Values( new ArrayList<Object>( rankings ) ) ) ;
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in update nodes view bolt:: ", e ) ; }
    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { declarer.declare( new Fields( "list" ) ) ; }    
    
	@Override
	public Map<String, Object> getComponentConfiguration() { return null ; }
}