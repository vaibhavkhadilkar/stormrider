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

import java.util.Map;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.utdallas.cs.stormrider.store.Store;
import edu.utdallas.cs.stormrider.store.StoreFactory;
import edu.utdallas.cs.stormrider.topology.TopologyException;
import edu.utdallas.cs.stormrider.views.ViewsFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class AnalyzeSpout implements IRichSpout 
{
	/** A default serial version uid **/
	private static final long serialVersionUID = 1L ;

    /** An output collector used to emit tuples from the Twitter stream **/
    private SpoutOutputCollector collector ;

	private Store store = null ;
	
	private long interval = 0L ;

	private String linkNameAsURI = null ;
	
    /** Constructor **/
    public AnalyzeSpout( long interval, boolean isReified, String storeConfigFile, String viewsConfigFile ) 
    { 
    	this.interval = interval ;
    	store = StoreFactory.getJenaHBaseStore( storeConfigFile, isReified ) ;
    	linkNameAsURI = ViewsFactory.getViews( viewsConfigFile ).getLinkNameAsURI() ;
    }
    
	@SuppressWarnings("rawtypes")
	@Override
    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) { this.collector = collector ; }

    @Override
    public void close() { }
    
    @Override
    public void nextTuple() 
    {
    	try
    	{
	    	long startTime = System.nanoTime() ;
	   		while( true )
	   		{
   				long currTime = System.nanoTime() ;
   				if( ( currTime - startTime ) >= interval )
   				{
   					ResultSet rs = store.getNodesForAnalysis( linkNameAsURI ) ;
   					while( rs.hasNext() )
   					{
   						QuerySolution qs = rs.next() ;
   						collector.emit( new Values( qs.getResource( qs.varNames().next() ).getLocalName().toString() ) ) ;
   					}
   					startTime = currTime ;
	   			}
   				Thread.sleep( 10000 ) ;
	   		}
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in analyze spout:: ", e ) ; }
    }
    
    @Override
    public void ack( Object msgId ) { }

    @Override
    public void fail( Object msgId ) { }
    
    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { declarer.declare( new Fields( "Node" ) ) ; }    
    
	@Override
	public void activate() { }

	@Override
	public void deactivate() { }

	@Override
	public Map<String, Object> getComponentConfiguration() { return null ; }
}