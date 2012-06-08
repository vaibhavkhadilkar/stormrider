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

package edu.utdallas.cs.stormrider.connection;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormRiderConnection 
{
    private static Logger LOG = LoggerFactory.getLogger( StormRiderConnection.class ) ;

    private Configuration config = null ;
    
	private HBaseAdmin admin = null ;
	
	private static final long CLIENT_CACHE_SIZE = 20971520 ;
	
	public StormRiderConnection( String configFile )
	{
		this.config = StormRiderConnectionFactory.createHBaseConfiguration( configFile ) ; this.config.setQuietMode( true ) ;
		this.admin = StormRiderConnectionFactory.createHBaseAdmin( config ) ;
	}
	
	public StormRiderConnection( Configuration config )
	{
		this.config = config ; this.config.setQuietMode( true ) ;
		this.admin = StormRiderConnectionFactory.createHBaseAdmin( config ) ;
	}
		
	public Configuration getConfiguration() { return config ; }
	
	public static StormRiderConnection none() { return new StormRiderConnection( "none" ) ; }
	
	public boolean hasAdminConnection() { return admin != null ; }
	
	public HBaseAdmin getAdmin() { return admin ; }
	
	public boolean doesTableExist( String tableName ) 
	{
		boolean tableExists = false ;
		try
		{
			tableExists = admin.tableExists( tableName ) ;
		}
		catch( Exception e ) { exception( "tableExists", e, tableName ) ; }
		return tableExists ;
	}
	
	public HTable openTable( String tableName )
	{
		HTable table = null ;
		try
		{
			
			admin.enableTable( tableName ) ;
			table = new HTable( config, tableName ) ;
			table.setAutoFlush( false ) ;
			table.setWriteBufferSize( CLIENT_CACHE_SIZE ) ;
		}
		catch( Exception e ) { exception( "openTable", e, tableName ) ; }
		return table ;
	}
	
	public void deleteTable( String tableName ) 
	{
		try
		{
			if( admin.tableExists( tableName ) )
			{
				admin.disableTable( tableName ) ;
				admin.deleteTable( tableName ) ;
			}
		}
		catch( Exception e ) { exception( "deleteTable", e, tableName ) ;  }
	}
	
	public HTable createTable( HTableDescriptor tableDesc )
	{
		HTable table = null ;
		try
		{
			admin.createTable( tableDesc ) ;
			admin.enableTable( tableDesc.getNameAsString() ) ;
			table = new HTable( config, tableDesc.getNameAsString() ) ;
			table.setAutoFlush( true ) ;
			table.setWriteBufferSize( CLIENT_CACHE_SIZE ) ;
		}
		catch( Exception e ) { exception( "createTable", e, tableDesc.getNameAsString() ) ; }
		return table ;
	}
	
	public HTable createTable( String tableName, List<String> columnNames )
	{
		HTable table = null ; 
		try
		{
			HTableDescriptor tableDescriptor = new HTableDescriptor( tableName ) ;
			admin.createTable( tableDescriptor ) ;
			admin.disableTable( tableName ) ;
			
			for( int i = 0; i < columnNames.size(); i++ )
			{
				HColumnDescriptor columnDescriptor = new HColumnDescriptor( columnNames.get( i ) ) ;
				columnDescriptor.setMaxVersions( Integer.MAX_VALUE );
				admin.addColumn( tableName, columnDescriptor ) ;
			}
			admin.enableTable( tableName ) ;
			table = new HTable( config, tableName ) ;
			table.setAutoFlush( true ) ;
			table.setWriteBufferSize( CLIENT_CACHE_SIZE ) ;
		}
		catch( Exception e ) { exception( "createTable", e, tableName ); }
		return table ;
	}
	
	private void exception( String who, Exception e, String tableName )
	{
		LOG.info( who + ": Exception \n " + e.getMessage() + " \n " + tableName ) ;
	}
}