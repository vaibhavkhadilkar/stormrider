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

import java.util.HashMap;
import java.util.Map;

import edu.utdallas.cs.stormrider.topology.TopologyException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountUserDegreeBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

    /** An output collector used to emit tuples from the Twitter stream **/
    private OutputCollector collector ;

	private Map<String, Long> mapNodeToDegree = new HashMap<String, Long>() ;
	
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare( Map conf, TopologyContext context, OutputCollector collector) { this.collector = collector ; }

    @Override
    public void execute( Tuple input ) 
    {
    	try
    	{
    		if( input.getString( 0 ).equals( "nlv" ) )
    		{
    			String node = input.getString( 1 ) ;
    			Long degree = mapNodeToDegree.get( node ) ;
    			if( degree == null ) mapNodeToDegree.put( node, new Long( 1 ) ) ;
    			else mapNodeToDegree.put( node, degree + 1 ) ;
    			collector.emit( new Values( node, mapNodeToDegree.get( node ) ) ) ;
    		}
    	}
        catch( Exception e ) { throw new TopologyException( "Exception in update nodes view bolt:: ", e ) ; }
    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) 
    { 
        declarer.declare( new Fields( "node" ) ) ;
        declarer.declare( new Fields( "degree" ) ) ;
    }    
}