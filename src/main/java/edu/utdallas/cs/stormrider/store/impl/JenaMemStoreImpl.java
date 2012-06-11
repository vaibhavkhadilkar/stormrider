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

package edu.utdallas.cs.stormrider.store.impl;

import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.utdallas.cs.stormrider.store.StoreBase;
import edu.utdallas.cs.stormrider.util.StormRiderConstants;

public class JenaMemStoreImpl extends StoreBase
{
	public JenaMemStoreImpl() { this( false ) ; }
	
	public JenaMemStoreImpl( boolean isReified ) 
	{ 
		this.isReified = isReified ;
		model = ModelFactory.createDefaultModel() ;
		if( isReified ) model.setNsPrefix( StormRiderConstants.REIFIED_STATEMENT_NS, StormRiderConstants.REIFIED_STATEMENT_URI ) ;
	}
}
