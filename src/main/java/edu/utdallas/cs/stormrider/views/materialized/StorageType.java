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

import java.util.List;

import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.util.Named;
import com.hp.hpl.jena.sparql.util.Symbol;
import com.talis.hbase.rdf.shared.SymbolRegistry;

import edu.utdallas.cs.stormrider.StormRiderException;

public class StorageType extends Symbol implements Named
{
    public static final StorageType StorageHBase = new StorageType( "hbase" ) ;

    static SymbolRegistry<StorageType> registry = new SymbolRegistry<StorageType>() ;
    static { init() ; }
    
    public static StorageType fetch( String storageTypeName )
    {
        if ( storageTypeName == null )
            throw new IllegalArgumentException( "LayoutType.convert: null not allowed" ) ;
        
        StorageType t = registry.lookup( storageTypeName ) ;
        if ( t != null ) return t ;

        LoggerFactory.getLogger( StorageType.class ).warn( "Can't turn '" + storageTypeName + "' into a storage type" ) ;
        throw new StormRiderException( "Can't turn '" + storageTypeName + "' into a storage type" ) ; 
    }
    
    static void init()
    {
        register( StorageHBase ) ;
    }
    
    static public List<String> allNames() { return registry.allNames() ; }
    static public List<StorageType> allTypes() { return registry.allSymbols() ; }
    
    static public void register( String name )
    {
        if ( name == null )
            throw new IllegalArgumentException( "StorageType.register(String): null not allowed" ) ;
        register( new StorageType( name ) ) ; 
    }
    
    static public void register( StorageType storageType )
    {
        if ( storageType == null )
            throw new IllegalArgumentException( "StorageType.register(LayoutType): null not allowed" ) ;
        registry.register( storageType ) ; 
    }

    static public void registerName( String storageName, StorageType storageType )
    {
        if ( storageType == null )
            throw new IllegalArgumentException( "StorageType.registerName: null not allowed" ) ;
        registry.register( storageName, storageType ) ; 
    }

    private StorageType( String storageName ) { super( storageName ) ; }

    public String getName() { return super.getSymbol() ; }
}