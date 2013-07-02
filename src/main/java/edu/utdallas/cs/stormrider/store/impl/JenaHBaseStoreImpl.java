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

package edu.utdallas.cs.stormrider.store.impl;

import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;

import edu.utdallas.cs.stormrider.store.StoreBase;
import edu.utdallas.cs.stormrider.util.StormRiderConstants;

public class JenaHBaseStoreImpl extends StoreBase
{
	private static final long serialVersionUID = 6058675792601078438L;

	public JenaHBaseStoreImpl( String configFile )
	{
		this( configFile, "", false ) ;
	}
	
	public JenaHBaseStoreImpl( String configFile, boolean isReified )
	{
		this( configFile, "", isReified ) ;
	}
	
	public JenaHBaseStoreImpl( String configFile, String iri, boolean isReified )
	{
		this( configFile, iri, isReified, false ) ;
	}
	
	public JenaHBaseStoreImpl( String configFile, String iri, boolean isReified, boolean formatStore )
	{
		super( configFile, iri, isReified, formatStore ) ;
		init( configFile, iri, isReified, formatStore ) ;
	}
	
	public void init( String configFile, String iri, boolean isReified, boolean formatStore )
	{
		Store store = HBaseRdfFactory.connectStore( configFile ) ;	
		if( formatStore ) store.getTableFormatter().format() ;
		
		if( iri.equals( "" ) )
			model = HBaseRdfFactory.connectDefaultModel( store ) ;
		else 
			model = HBaseRdfFactory.connectNamedModel( store, iri ) ;
		model.setNsPrefix( StormRiderConstants.REIFIED_STATEMENT_NS, StormRiderConstants.REIFIED_STATEMENT_URI ) ;				
	}
}