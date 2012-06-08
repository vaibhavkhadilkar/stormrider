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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import edu.utdallas.cs.stormrider.topology.TopologyException;
import edu.utdallas.cs.stormrider.util.StormRiderConstants;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class QueryBolt implements IRichBolt 
{
	private static final long serialVersionUID = 1L ;

	private HTable resultTable = null ;
	
	private Configuration hbaseConfig = null ;
	
	public QueryBolt( String hbaseConfigFile, String resultTableName ) 
	{ 
		hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.addResource( new Path( hbaseConfigFile ) );
		createResultTable( resultTableName ) ;
	}
	
	private void createResultTable( String resultTableName )
	{
		try
		{
			HBaseAdmin admin = new HBaseAdmin( hbaseConfig ) ;
	
			if( admin.tableExists( resultTableName ) )
			{
				admin.disableTable( resultTableName ) ;
				admin.deleteTable( resultTableName ) ;
			}
			
			if( !admin.tableExists( resultTableName ) ) 
			{
				HTableDescriptor tableDescriptor = new HTableDescriptor( resultTableName ) ;
				admin.createTable( tableDescriptor ) ;
				admin.disableTable( resultTableName ) ;
			
				HColumnDescriptor resultColDesc = new HColumnDescriptor( StormRiderConstants.colFamResults ) ;
				resultColDesc.setMaxVersions( Integer.MAX_VALUE ) ;
				admin.addColumn( resultTableName, resultColDesc ) ;
				
				admin.enableTable( resultTableName ) ;
			}
			resultTable = new HTable( hbaseConfig, resultTableName ) ;
			resultTable.setAutoFlush( true ) ;
		}
        catch( Exception e ) { throw new TopologyException( "Exception during hbase result table creation in query bolt:: ", e ) ; }
	}
	
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare( Map conf, TopologyContext context, OutputCollector collector) { }

    @Override
    public void execute( Tuple input ) 
    {
    	try
    	{
    		long numOfVars = input.size() ;
    		for( int i = 0 ; i < numOfVars ; i++ )
    		{
    			byte[] rowBytes = Bytes.toBytes( i ), colFamilyBytes = Bytes.toBytes( StormRiderConstants.colFamResults + ":Var" + (i+1) ),
    			       colQualBytes = Bytes.toBytes( input.getString( i ) ) ;
    			Put update = new Put( rowBytes ) ;
    			update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ;
    			resultTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, null, update ) ;
    		}
    	}
        catch( Exception e ) { throw new TopologyException( "Exception during hbase result table updation in query bolt:: ", e ) ; }
    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { }    
}