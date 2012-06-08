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

import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;
import com.hp.hpl.jena.util.FileManager;

import edu.utdallas.cs.stormrider.StormRiderException;
import edu.utdallas.cs.stormrider.assembler.AssemblerVocab;

public class StormRiderConnectionDesc 
{
	private String config = null ;
	
    public static StormRiderConnectionDesc blank()
    { return new StormRiderConnectionDesc() ; }
    
    public static StormRiderConnectionDesc none()
    {
    	StormRiderConnectionDesc x = new StormRiderConnectionDesc() ;
        x.config = "none" ;
        return x ;
    }

    private StormRiderConnectionDesc() {}
    
    public static StormRiderConnectionDesc read( String filename )
    {
        Model m = FileManager.get().loadModel( filename ) ;
        return worker( m ) ;
    }
    
    public static StormRiderConnectionDesc read( Model m ) { return worker( m ) ; }
    
    private static StormRiderConnectionDesc worker( Model m )
    {
        Resource r = GraphUtils.getResourceByType( m, AssemblerVocab.HBaseConnectionAssemblerType ) ;
        if ( r == null )
            throw new StormRiderException( "Can't find connection description" ) ;
        StormRiderConnectionDesc desc = ( StormRiderConnectionDesc )AssemblerBase.general.open( r ) ;
        return desc ;
    }

    public void setConfig( String config ) { this.config = config ; }
    
    public String getConfig() { return config ; }
}
