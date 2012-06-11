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

import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;

import edu.utdallas.cs.stormrider.store.StoreBase;
import edu.utdallas.cs.stormrider.util.StormRiderConstants;

public class JenaHBaseStoreImpl extends StoreBase
{
	public JenaHBaseStoreImpl( String configFile )
	{
		this( configFile, false ) ;
	}
	
	public JenaHBaseStoreImpl( String configFile, boolean isReified )
	{
		this.isReified = isReified ;
		Store store = HBaseRdfFactory.connectStore( configFile ) ;		
		model = HBaseRdfFactory.connectDefaultModel( store ) ;
		model.setNsPrefix( StormRiderConstants.REIFIED_STATEMENT_NS, StormRiderConstants.REIFIED_STATEMENT_URI ) ;		
	}
}