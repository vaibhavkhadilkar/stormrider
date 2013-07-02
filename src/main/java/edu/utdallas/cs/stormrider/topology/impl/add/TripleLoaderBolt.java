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

import java.util.Map;

import org.apache.log4j.Logger;

import edu.utdallas.cs.stormrider.store.Store;
import edu.utdallas.cs.stormrider.store.StoreFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TripleLoaderBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

	/** A Logger for this class **/
    private static Logger LOG = Logger.getLogger( TripleLoaderBolt.class ) ;

    private boolean isReified = false ;
    
    private boolean formatStore = false ;
    
    private String storeConfigFile = null ;
    
    private String iri = null ;

	public TripleLoaderBolt( boolean isReified, String storeConfigFile ) { this( isReified, storeConfigFile, "" ) ; }
	
	public TripleLoaderBolt( boolean isReified, String storeConfigFile, String iri ) { this( isReified, storeConfigFile, iri, false ) ; }

	public TripleLoaderBolt( boolean isReified, String storeConfigFile, String iri, boolean formatStore ) 
	{ 
		this.isReified = isReified ;
		this.formatStore = formatStore ;
		this.storeConfigFile = storeConfigFile ;
		this.iri = iri ;
	}

	@SuppressWarnings("rawtypes")
	@Override
    public void prepare( Map conf, TopologyContext context, OutputCollector collector) { }

    @Override
    public void execute( Tuple input ) 
    {
    	try
    	{
    		Store store = StoreFactory.getJenaHBaseStore( storeConfigFile, iri, isReified, formatStore ) ; 
	    	if( input.getString( 0 ).equals( "triple" ) )
	    		store.addTriple( input.getString( 1 ), input.getString( 2 ), input.getString( 3 ) ) ;
    	}
    	catch( Exception e ) { LOG.info( "Error in adding triple in bolt", e ) ; }
    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { }    
    
	@Override
	public Map<String, Object> getComponentConfiguration() { return null ; }
}