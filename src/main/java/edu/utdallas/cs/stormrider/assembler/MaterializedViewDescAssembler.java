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

package edu.utdallas.cs.stormrider.assembler;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;

import edu.utdallas.cs.stormrider.connection.StormRiderConnectionDesc;
import edu.utdallas.cs.stormrider.views.materialized.MaterializedViewsDesc;

public class MaterializedViewDescAssembler extends AssemblerBase implements Assembler
{
	@Override
    public MaterializedViewsDesc open( Assembler a, Resource root, Mode mode )
    {
    	StormRiderConnectionDesc hbaseConnDesc = null ;
        Resource c = GraphUtils.getResourceValue( root, AssemblerVocab.pConnection ) ;
        if ( c != null )
            hbaseConnDesc = (StormRiderConnectionDesc)a.open( c ) ;
        
        String storageName = GraphUtils.getStringValue( root, AssemblerVocab.pStorage ) ;
        String name = GraphUtils.getStringValue( root, AssemblerVocab.pName ) ;
        
        MaterializedViewsDesc storeDesc = new MaterializedViewsDesc( storageName, name ) ; 
        storeDesc.connDesc = hbaseConnDesc ;
        
        return storeDesc;
    }    
}