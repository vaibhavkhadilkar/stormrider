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

package edu.utdallas.cs.stormrider;

import com.hp.hpl.jena.assembler.assemblers.AssemblerGroup;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.impl.PrefixMappingImpl;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import edu.utdallas.cs.stormrider.assembler.AssemblerVocab;

public class StormRider 
{
    public final static String namespace = "http://cs.utdallas.edu/semanticweb/stormrider/2012/stormrider#" ;
    
    static { initWorker() ; }
    public static void init() { }
    
    /** Used by Jena assemblers for registration */ 
    public static void whenRequiredByAssembler( AssemblerGroup g )
    {
        AssemblerVocab.register( g ) ;
    }
    
    private static boolean initialized = false ;
    private static synchronized void initWorker()
    {
        if ( initialized )
            return ;
        
        initialized = true ;        
    }
    
    /** RDF namespace prefix */
    private static final String rdfPrefix = RDF.getURI() ;

    /** RDFS namespace prefix */
    private static final String rdfsPrefix = RDFS.getURI() ;

    /** OWL namespace prefix */
    private static final String owlPrefix = OWL.getURI() ;
    
    /** XSD namespace prefix */
    private static final String xsdPrefix = XSDDatatype.XSD + "#" ;
    
    protected static PrefixMapping globalPrefixMap = new PrefixMappingImpl() ;
    static 
    {
        globalPrefixMap.setNsPrefix( "rdf",  rdfPrefix ) ;
        globalPrefixMap.setNsPrefix( "rdfs", rdfsPrefix ) ;
        globalPrefixMap.setNsPrefix( "xsd",  xsdPrefix ) ;
        globalPrefixMap.setNsPrefix( "owl" , owlPrefix ) ;
    }
    public static PrefixMapping getGlobalPrefixMapping() { return globalPrefixMap ; }

}
