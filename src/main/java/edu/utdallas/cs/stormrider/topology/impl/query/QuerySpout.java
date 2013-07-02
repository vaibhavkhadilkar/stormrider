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

package edu.utdallas.cs.stormrider.topology.impl.query;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
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

	/** A Logger for this class **/
    private static Logger LOG = Logger.getLogger( QuerySpout.class ) ;
    		
    /** An output collector used to emit tuples from the Twitter stream **/
    private SpoutOutputCollector collector ;

    private String queryString = null ;

    private String configFile = null ;
    
    private String iri = null ;
    
    private boolean isReified = false ;
    
	private long numOfReports = 0L, maxReports = 0L, interval = 0L ;

	private long startTime = 0L, currTime = 0L ;
	
	private transient Query query = null ;
	
	/** Constructor **/
    public QuerySpout( long maxReports, long interval, String queryString, boolean isReified, String configFile ) 
    { this( maxReports, interval, queryString, isReified, configFile, "" ) ; }

	public QuerySpout( long maxReports, long interval, String queryString, boolean isReified, String configFile, String iri )
	{
    	this.maxReports = maxReports ;
    	this.interval = interval ;
    	this.queryString = queryString ;
    	this.isReified = isReified ;
    	this.configFile = configFile ;
    	this.iri = iri ;
    	this.startTime = System.nanoTime() ;
    	this.query = QueryFactory.create( queryString ) ;
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
   			if( numOfReports < maxReports )
   			{
   				currTime = System.nanoTime() ;
	   			LOG.info( "startTime: " + startTime + " currTime: " + currTime + " interval: " + ( ( currTime - startTime ) * 1e-6 ) ) ;
   				if( ( ( currTime - startTime ) * 1e-6 ) >= interval )
   				{
   		   			if( query == null ) query = QueryFactory.create( queryString ) ;
   		   			Store store = StoreFactory.getJenaHBaseStore( configFile, iri, isReified ) ;
   					int solnCount = 0 ;
   					ResultSet rs = store.executeSelectQuery( query ) ;
   					List<Var> listVars = query.getProject().getVars() ;
   					while( rs.hasNext() )
   					{
   						solnCount++ ;
   						QuerySolution qs = rs.next() ;
   						Object[] results = new String[listVars.size()+1] ;
   						if( solnCount == 1 ) results[0] = "new" ;
   						else results[0] = "cont" ;
   						for( int i = 1 ; i <= listVars.size() ; i++ )
   						{
   							Var var = listVars.get( i-1 ) ;
   							RDFNode value = qs.get( var.toString() ) ;
   							if( value.isResource() || value.isAnon() ) results[i] = value.asResource().toString() ;
   							else if( value.isLiteral() ) results[i] = value.asLiteral().toString() ;
   						}
   						collector.emit( new Values( results ) ) ;
   					}
   					numOfReports += 1 ;
   					startTime = currTime ;
   				}
   				Thread.sleep( 30000 ) ;
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
    	String[] fields = new String[query.getResultVars().size()+1] ;
    	fields[0] = "msg" ;
    	for( int i = 1 ; i <= query.getResultVars().size() ; i++ )
    		fields[i] = "V" + i  ;
		declarer.declare( new Fields( fields ) ) ;
    }    
    
	@Override
	public void activate() { }

	@Override
	public void deactivate() { }

	@Override
	public Map<String, Object> getComponentConfiguration() { return null ; }
}