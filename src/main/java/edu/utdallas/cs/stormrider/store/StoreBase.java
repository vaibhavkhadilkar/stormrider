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

package edu.utdallas.cs.stormrider.store;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ReifiedStatement;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

import edu.utdallas.cs.stormrider.store.iterator.impl.NodeAndNeighbor;
import edu.utdallas.cs.stormrider.store.iterator.impl.NodesAndNeighborsIterator;
import edu.utdallas.cs.stormrider.util.StormRiderConstants;
import edu.utdallas.cs.stormrider.vocab.STORMRIDER;

public abstract class StoreBase implements Store 
{
	private static final long serialVersionUID = -8213117060840813344L;

	protected Model model = null ;
	
	protected String configFile = null ;
	
	protected String iri = null ;
	
	protected boolean isReified = false ;
	
	protected boolean formatStore = false ;
	
	public StoreBase( String configFile, String iri, boolean isReified, boolean formatStore )
	{
		this.configFile = configFile ;
		this.iri = iri ;
		this.isReified = isReified ;
		this.formatStore = formatStore ;
	}
	
	public abstract void init( String configFile, String iri, boolean isReified, boolean formatStore ) ;
	
	private void addReifiedStatement( StringBuilder sb, Statement s, long currTime )
	{
		ReifiedStatement rs = model.createReifiedStatement( sb.toString(), s ) ;
		model.add( rs, STORMRIDER.Timestamp, model.createTypedLiteral( currTime ) ) ;
		rs = null ;
	}
	
	public void addTriple( String subject, String predicate, String object )
	{
		try
		{
			if( model == null ) init( configFile, iri, isReified, formatStore ) ;
			long currTime = System.nanoTime() ;
			StringBuilder sb = new StringBuilder() ; 
			if( isReified ) { sb.append( model.getNsPrefixURI( StormRiderConstants.REIFIED_STATEMENT_NS ) ) ; sb.append( currTime ) ; }
			Resource subj = model.createResource( subject ) ;
			Property pred = model.createProperty( predicate ) ;
			String[] objSplit  = object.split( "~" ) ;
			if( objSplit[0].startsWith( "res" ) )
			{
				Resource obj = model.createResource( objSplit[1] ) ;
				Statement s  = model.createStatement( subj, pred, obj ) ; 
				model.add( s ) ;
				if( isReified ) addReifiedStatement( sb, s, currTime ) ; 
				s = null ;
			}
			else if( objSplit[0].startsWith( "lit" ) )
			{
				RDFNode obj = model.createLiteral( objSplit[1] ) ;
				Statement s = model.createStatement( subj, pred, obj ) ; 
				model.add( s ) ;
				if( isReified ) addReifiedStatement( sb, s, currTime ) ; 
				s = null ;
			}
		}
		catch( Exception e ) { throw new StoreException( "Exception in adding triple", e ) ; }
	}
	
	public ResultSet executeSelectQuery( Query query )
	{
		try
		{
			if( model == null ) init( configFile, iri, isReified, formatStore ) ;
			QueryExecution qExec = QueryExecutionFactory.create( query, model ) ;
			return qExec.execSelect() ;
		}
		catch( Exception e ) { throw new StoreException( "Exception in executing selecting query", e ) ; }
	}

	public Iterator<NodeAndNeighbor> getAllNodesWithNeighbors( String linkNameAsURI )
	{
		try
		{
			if( model == null ) init( configFile, iri, isReified, formatStore ) ;
			String queryString =
			" SELECT ?x ?y " +
			" WHERE { ?x <" + linkNameAsURI + "> ?y } " ;
			QueryExecution qExec = QueryExecutionFactory.create( queryString, model ) ;
			ResultSet rs = qExec.execSelect() ;
			return new NodesAndNeighborsIterator( rs ) ;
		}
		catch( Exception e ) { throw new StoreException( "Exception in getting all nodes and neighbors", e ) ; }
	}
	
	public String getAdjacencyList( String nodeAsURI, String linkNameAsURI )
	{
		try
		{
			if( model == null ) init( configFile, iri, isReified, formatStore ) ;
			StringBuilder sbAdjList = new StringBuilder() ;
			String queryString =
			" SELECT ?y " +
			" WHERE { <" + nodeAsURI + "> <" + linkNameAsURI + "> ?y } " ;
			QueryExecution qExec = QueryExecutionFactory.create( queryString, model ) ;
			ResultSet rs = qExec.execSelect() ;
			while( rs.hasNext() )
			{
				sbAdjList.append( rs.next().getResource( "?y" ) ) ; sbAdjList.append( "~" ) ;
			}
			return sbAdjList.toString() ;
		}
		catch( Exception e ) { throw new StoreException( "Exception in getting adjacency list", e ) ; }	
	}
	
	public ResultSet getNodesForAnalysis( String linkNameAsURI )
	{
		try
		{
			if( model == null ) init( configFile, iri, isReified, formatStore ) ;
			String queryString =
			" SELECT DISTINCT ?x " +
			" WHERE { ?x <" + linkNameAsURI + "> ?y } " ;
			QueryExecution qExec = QueryExecutionFactory.create( queryString, model ) ;
			return qExec.execSelect() ;
		}
		catch( Exception e ) { throw new StoreException( "Exception in getting nodes for analysis", e ) ; }
	}
	
	@Override
	public String toString()
	{
		return "[ configFile = " + configFile + ", iri = " + iri + ", isReified = " + isReified + ", formatStore = " + formatStore + " ]" ;
	}
}