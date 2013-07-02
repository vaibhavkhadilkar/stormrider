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

package edu.utdallas.cs.stormrider.views.materialized;

import static edu.utdallas.cs.stormrider.views.materialized.StorageType.*;
import static java.lang.String.format ;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utdallas.cs.stormrider.StormRider;
import edu.utdallas.cs.stormrider.connection.StormRiderConnection;
import edu.utdallas.cs.stormrider.connection.StormRiderConnectionDesc;
import edu.utdallas.cs.stormrider.connection.StormRiderConnectionFactory;
import edu.utdallas.cs.stormrider.views.materialized.impl.hbase.HBaseMaterializedViews;

public class MaterializedViewsFactory 
{
    private static Logger log = LoggerFactory.getLogger( MaterializedViewsFactory.class ) ;
    
    static { StormRider.init() ; } 

	public static MaterializedViews create( String configFile ) { return create( MaterializedViewsDesc.read( configFile ), null ) ; }
	
    public static MaterializedViews create( StormRiderConnection conn, StorageType storage, String name )
    { 
    	MaterializedViewsDesc desc = new MaterializedViewsDesc( storage, name ) ;
        return create( desc, conn ) ;
    }

    public static MaterializedViews create( StorageType storage, String name )
    { 
    	MaterializedViewsDesc desc = new MaterializedViewsDesc( storage, name ) ;
        return create( desc, null ) ;
    }

    public static MaterializedViews create( MaterializedViewsDesc desc )
    { return create( desc, null ) ; }

	public static MaterializedViews create( MaterializedViewsDesc mvDesc, StormRiderConnection conn )
	{
        MaterializedViews mvs = _create( conn, mvDesc ) ;
        return mvs ;
    }
    
    private static MaterializedViews _create( StormRiderConnection conn, MaterializedViewsDesc desc )
    {
        if ( conn == null && desc.connDesc == null )
            desc.connDesc = StormRiderConnectionDesc.none() ;

        if ( conn == null && desc.connDesc != null)
            conn = StormRiderConnectionFactory.create( desc.connDesc ) ;
        
        StorageType storageType = desc.getStorage() ;
        return _create( desc, conn, storageType ) ;        
    }
    
    private static MaterializedViews _create( MaterializedViewsDesc desc, StormRiderConnection conn, StorageType storageType )
    {
    	MaterializedViewsMaker f = registry.get( storageType ) ;
        if ( f == null )
        {
            log.warn( format( "No factory for %s", storageType.getName() ) ) ;
            return null ;
        }       
        return f.create( conn, desc ) ;
    }
 
    public static void register( StorageType storageType, MaterializedViewsMaker factory )
    {
        registry.put( storageType, factory ) ;
    }
    
    private static Map<StorageType, MaterializedViewsMaker> registry = new HashMap<StorageType, MaterializedViewsMaker>() ;

    static { setRegistry() ;  }

    static private void setRegistry()
    {
        register( StorageHBase, 
                	new MaterializedViewsMaker() {
                    public MaterializedViews create( StormRiderConnection conn, MaterializedViewsDesc desc )
                    { return new HBaseMaterializedViews( conn, desc ) ; } } ) ;
    }
}