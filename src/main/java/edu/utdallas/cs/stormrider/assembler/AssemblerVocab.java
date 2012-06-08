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

package edu.utdallas.cs.stormrider.assembler;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.assemblers.AssemblerGroup;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.talis.hbase.rdf.util.Vocab;

import edu.utdallas.cs.stormrider.StormRider;

public class AssemblerVocab 
{
    private static final String NS = StormRider.namespace ;
    
    public static String getURI() { return NS ; } 

    // Types
    public static final Resource HBaseConnectionAssemblerType   = Vocab.type( NS, "HBaseConnection" ) ;
    public static final Resource ViewAssemblerType              = Vocab.type( NS, "View" ) ;

    // ---- View
    public static final Property pConnection         			= Vocab.property( NS, "connection" ) ;
    public static final Property pStorage             			= Vocab.property( NS, "storage" ) ;
    public static final Property pName							= Vocab.property( NS, "name" ) ;

    // ---- Connection
    public static final Property pStormRiderConfiguration       = Vocab.property( NS, "configuration" ) ;
    
    private static boolean initialized = false ; 
    
    static { init() ; }
    
    static public void init()
    {
        if ( initialized )
            return ;
        register( Assembler.general ) ;
        initialized = true ;
    }
    
    static public void register( AssemblerGroup g )
    {
        assemblerClass( g, HBaseConnectionAssemblerType,  new StormRiderConnectionDescAssembler() ) ;
        assemblerClass( g, ViewAssemblerType,             new MaterializedViewDescAssembler() ) ;
    }
    
    private static void assemblerClass( AssemblerGroup g, Resource r, Assembler a )
    {
        if ( g == null )
            g = Assembler.general ;
        g.implementWith( r, a ) ;
    }
}