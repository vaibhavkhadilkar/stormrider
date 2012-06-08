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

package edu.utdallas.cs.stormrider.views.materialized;

import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;
import com.hp.hpl.jena.util.FileManager;

import edu.utdallas.cs.stormrider.StormRiderException;
import edu.utdallas.cs.stormrider.assembler.AssemblerVocab;
import edu.utdallas.cs.stormrider.connection.StormRiderConnectionDesc;

public class MaterializedViewsDesc 
{
    public StormRiderConnectionDesc connDesc   	= null ;
    private StorageType storage				= null ;
    private String name							= null ;
    
    public static MaterializedViewsDesc read( String filename )
    {
        Model m = FileManager.get().loadModel( filename ) ;
        return read( m ) ;
    }
    
    public MaterializedViewsDesc( String storageName, String name )
    {
        this( StorageType.fetch( storageName ), name ) ;
    }
    
    public MaterializedViewsDesc( StorageType storage, String name )
    {
        this.storage = storage ;
        if( name == null ) this.name = "default" ;
        else this.name = name ;
    }
    
    public String getLinkName() { return name ; }
    
    public void setLinkName( String name ) { this.name = name ; }
    
    public StorageType getStorage() { return storage ; }
    
    public void setLayout( StorageType storage ) { this.storage = storage ; }

    public static MaterializedViewsDesc read( Model m )
    {
        // Does not mind store descriptions or dataset descriptions
        Resource r = GraphUtils.getResourceByType( m, AssemblerVocab.ViewAssemblerType ) ;
        
        if ( r == null )
            throw new StormRiderException( "Can't find store description" ) ;
        return read( r ) ;
    }

    public static MaterializedViewsDesc read( Resource r )
    {
        return (MaterializedViewsDesc)AssemblerBase.general.open( r ) ;
    }
}