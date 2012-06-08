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

package edu.utdallas.cs.stormrider.store;

import java.util.Calendar;

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

import edu.utdallas.cs.stormrider.util.StormRiderConstants;
import edu.utdallas.cs.stormrider.vocab.STORMRIDER;

public abstract class StoreBase implements Store 
{
	protected Model model = null ;
	
	protected boolean isReified = false ;
	
	private void addReifiedStatement( StringBuilder sb, Statement s, Calendar currTime )
	{
		ReifiedStatement rs = model.createReifiedStatement( sb.toString(), s ) ;
		model.add( rs, STORMRIDER.Timestamp, model.createTypedLiteral( currTime ) ) ;
		rs = null ;
	}
	
	public void addTriple( String subject, String predicate, String object )
	{
		Calendar currTime = Calendar.getInstance() ;
		StringBuilder sb = new StringBuilder() ; 
		if( isReified ) sb.append( model.getNsPrefixURI( StormRiderConstants.REIFIED_STATEMENT_NS ) ) ; sb.append( currTime.getTimeInMillis() ) ;
		Resource subj = model.createResource( subject ) ;
		Property pred = model.createProperty( predicate ) ;
		String[] objSplit  = object.split( "~" ) ;
		if( objSplit[0].startsWith( "res" ) )
		{
			Resource obj = model.createResource( objSplit[1] ) ;
			Statement s  = model.createStatement( subj, pred, obj ) ; model.add( s ) ;
			addReifiedStatement( sb, s, currTime ) ; s = null ;
		}
		else if( objSplit[0].startsWith( "lit" ) )
		{
			RDFNode obj = model.createLiteral( objSplit[1] ) ;
			Statement s = model.createStatement( subj, pred, obj ) ; model.add( s ) ;
			addReifiedStatement( sb, s, currTime ) ; s = null ;
		}
	}
	
	public ResultSet executeSelectQuery( Query query )
	{
		QueryExecution qExec = QueryExecutionFactory.create( query, model ) ;
		return qExec.execSelect() ;
	}
	
	public String getAdjacencyList( String linkNameAsURI )
	{
		StringBuilder sbAdjList = new StringBuilder() ;
		String queryString =
		" SELECT ?y " +
		" WHERE { ?x <" + linkNameAsURI + "> ?y } " ;
		QueryExecution qExec = QueryExecutionFactory.create( queryString, model ) ;
		ResultSet rs = qExec.execSelect() ;
		while( rs.hasNext() )
		{
			sbAdjList.append( rs.next().getResource( "?y" ).getLocalName() ) ; sbAdjList.append( "~" ) ;
		}
		return sbAdjList.toString() ;
	}
	
	public ResultSet getNodesForAnalysis( String linkNameAsURI )
	{
		String queryString =
		" SELECT DISTINCT ?x " +
		" WHERE { ?x <" + linkNameAsURI + "> ?y } " ;
		QueryExecution qExec = QueryExecutionFactory.create( queryString, model ) ;
		return qExec.execSelect() ;
	}
}