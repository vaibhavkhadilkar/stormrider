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

package edu.utdallas.cs.stormrider.topology.impl.query;

import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.core.Var;

import edu.utdallas.cs.stormrider.store.Store;
import edu.utdallas.cs.stormrider.store.StoreFactory;
import edu.utdallas.cs.stormrider.topology.TopologyException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class QuerySpout implements IRichSpout 
{
	/** A default serial version uid **/
	private static final long serialVersionUID = 1L ;

    /** A variable that denotes if this spout is distributed **/
    private boolean isDistributed = false ;
    
    /** An output collector used to emit tuples from the Twitter stream **/
    private SpoutOutputCollector collector ;

	private Store store = null ;
	
	private long maxReports = 0L, interval = 0L ;

	private Query query = null ;
	
    /** Constructor **/
    public QuerySpout( boolean isDistributed, long maxReports, long interval, String queryString, boolean isReified, String configFile ) 
    { 
    	this.isDistributed = isDistributed ;
    	this.maxReports = maxReports ;
    	this.interval = interval ;
    	store = StoreFactory.getJenaHBaseStore( configFile, isReified ) ;
    	this.query = QueryFactory.create( queryString ) ;
    }
    
    @Override
    public boolean isDistributed() { return isDistributed ; }
    
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
	    	long numOfReports = 0L, startTime = System.nanoTime() ;
	   		while( true )
	   		{
	   			while( numOfReports < maxReports )
	   			{
	   				long currTime = System.nanoTime() ;
	   				if( ( currTime - startTime ) >= interval )
	   				{
	   					ResultSet rs = store.executeSelectQuery( query ) ;
	   					List<Var> listVars = query.getProject().getVars() ;
	   					while( rs.hasNext() )
	   					{
	   						QuerySolution qs = rs.next() ;
	   						Object[] results = new String[listVars.size()] ;
	   						for( int i = 0 ; i < listVars.size() ; i++ )
	   						{
	   							Var var = listVars.get( i ) ;
	   							if( var.isURI() || var.isBlank() ) results[i] = qs.getResource( var.toString() ).toString() ;
	   							else if( var.isLiteral() ) results[i] = qs.getLiteral( var.toString() ).toString() ;
	   						}
	   						collector.emit( new Values( results ) ) ;
	   					}
	   					numOfReports += 1 ;
	   					startTime = currTime ;
	   				}
	   				Thread.sleep( 10000 ) ;
	   			}
	   		}
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in query spout:: ", e ) ; }
    }
    
    @Override
    public void ack( Object msgId ) { }

    @Override
    public void fail( Object msgId ) { }
    
    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) 
    {
    	for( int i = 1 ; i <= query.getResultVars().size() ; i++ )
    		declarer.declare( new Fields( "V" + i ) ) ;
    }    
}