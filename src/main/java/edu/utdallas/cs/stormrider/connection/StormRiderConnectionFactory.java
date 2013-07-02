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

package edu.utdallas.cs.stormrider.connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import edu.utdallas.cs.stormrider.StormRiderException;

public class StormRiderConnectionFactory 
{
    public static StormRiderConnection create( StormRiderConnectionDesc desc ) { return worker( desc ) ; }

    public static StormRiderConnection create( String configFile, boolean isAssemblerFile )
    { 
    	if( isAssemblerFile )
    	{
    		StormRiderConnectionDesc desc = StormRiderConnectionDesc.read( configFile ) ;
    		return create( desc ) ;
    	}
    	else
        	return new StormRiderConnection( configFile ) ;    		
    }

    public static StormRiderConnection create( Configuration config ) { return new StormRiderConnection( config ) ; }
    
    private static StormRiderConnection worker( StormRiderConnectionDesc desc )
    { return makeHBaseConnection( desc ) ; }

    private static StormRiderConnection makeHBaseConnection( StormRiderConnectionDesc desc )
    {
    	StormRiderConnection c = new StormRiderConnection( createHBaseConfiguration( desc.getConfig() ) ) ;
        return c ;
    }

    public static HBaseAdmin createHBaseAdmin( StormRiderConnectionDesc desc )
    {
    	Configuration config = HBaseConfiguration.create() ;
    	config.addResource( desc.getConfig() ) ;
    	try { return new HBaseAdmin( config ) ; } 
    	catch ( Exception e ) { throw new StormRiderException( "HBase exception while creating admin" ) ; }     	
    }
    
    public static HBaseAdmin createHBaseAdmin( String configFile )
    {
    	Configuration config = HBaseConfiguration.create() ;
    	config.addResource( new Path( configFile ) ) ;
    	try { return new HBaseAdmin( config ) ; } 
    	catch ( Exception e ) { throw new StormRiderException( "HBase exception while creating admin" ) ; }     	    	
    }
    
    public static HBaseAdmin createHBaseAdmin( Configuration config )
    {
    	try { return new HBaseAdmin( config ) ; } 
    	catch ( Exception e ) { throw new StormRiderException( "HBase exception while creating admin" ) ; }    	
    }
    
    public static Configuration createHBaseConfiguration( String configFile )
    {
    	Configuration config = HBaseConfiguration.create() ;
    	config.addResource( new Path( configFile ) ) ;
    	return config ;
    }
}