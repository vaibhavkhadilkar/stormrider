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

package edu.utdallas.cs.stormrider.concepts;

import java.util.Calendar;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ReifiedStatement;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import edu.utdallas.cs.stormrider.util.StormRiderConstants;
import edu.utdallas.cs.stormrider.util.TwitterConstants;
import edu.utdallas.cs.stormrider.vocab.STORMRIDER;
import edu.utdallas.cs.stormrider.vocab.TWITTER;

public class TestVersionManagement 
{
	public static void main( String[] args ) throws InterruptedException
	{
		Model _model = ModelFactory.createDefaultModel() ;
        _model.setNsPrefix( StormRiderConstants.REIFIED_STATEMENT_NS, StormRiderConstants.REIFIED_STATEMENT_URI ) ;

		Resource _subject = _model.createResource( TwitterConstants.TWITTER_USER_URI + "123456" ) ;

		Calendar _currTime1 = Calendar.getInstance() ; 
		StringBuilder _sb1 = new StringBuilder() ; _sb1.append( _model.getNsPrefixURI( StormRiderConstants.REIFIED_STATEMENT_NS ) ) ; _sb1.append( "123456" ) ; _sb1.append( "-" ) ; _sb1.append( _currTime1.getTimeInMillis() ) ;

		Resource _friend1 = _model.createResource( TwitterConstants.TWITTER_USER_URI + "654321" ) ;
		Statement _s1 = _model.createStatement( _subject, TWITTER.Has_Friend, _friend1 ) ; _model.add( _s1 ) ;
		ReifiedStatement _rs1 = _model.createReifiedStatement( _sb1.toString(), _s1 ) ;
		_model.add( _rs1, STORMRIDER.Timestamp, _model.createTypedLiteral( _currTime1 ) ) ;
		_s1 = null ; _rs1 = null ;

		Thread.sleep( 10000 ) ;
		
		Calendar _currTime2 = Calendar.getInstance() ; 
		StringBuilder _sb2 = new StringBuilder() ; _sb2.append( _model.getNsPrefixURI( StormRiderConstants.REIFIED_STATEMENT_NS ) ) ; _sb2.append( "123456" ) ; _sb2.append( "-" ) ; _sb2.append( _currTime2.getTimeInMillis() ) ;
		
		Resource _friend2 = _model.createResource( TwitterConstants.TWITTER_USER_URI + "987654" ) ;
		Statement _s2 = _model.createStatement( _subject, TWITTER.Has_Friend, _friend2 ) ; _model.add( _s2 ) ;
		ReifiedStatement _rs2 = _model.createReifiedStatement( _sb2.toString(), _s2 ) ;
		_model.add( _rs2, STORMRIDER.Timestamp, _model.createTypedLiteral( _currTime2 ) ) ;
		_s2 = null ; _rs2 = null ;
		
		Thread.sleep( 10000 ) ;
		
		Calendar _currTime3 = Calendar.getInstance() ; 
		StringBuilder _sb3 = new StringBuilder() ; _sb3.append( _model.getNsPrefixURI( StormRiderConstants.REIFIED_STATEMENT_NS ) ) ; _sb3.append( "123456" ) ; _sb3.append( "-" ) ; _sb3.append( _currTime3.getTimeInMillis() ) ;
		
		Resource _friend3 = _model.createResource( TwitterConstants.TWITTER_USER_URI + "456987" ) ;
		Statement _s3 = _model.createStatement( _subject, TWITTER.Has_Friend, _friend3 ) ; _model.add( _s3 ) ;
		ReifiedStatement _rs3 = _model.createReifiedStatement( _sb3.toString(), _s3 ) ;
		_model.add( _rs3, STORMRIDER.Timestamp, _model.createTypedLiteral( _currTime3 ) ) ;
		_s3 = null ; _rs3 = null ;

		Thread.sleep( 10000 ) ;
		
		Calendar _currTime4 = Calendar.getInstance() ; 
		StringBuilder _sb4 = new StringBuilder() ; _sb4.append( _model.getNsPrefixURI( StormRiderConstants.REIFIED_STATEMENT_NS ) ) ; _sb4.append( "123456" ) ; _sb4.append( "-" ) ; _sb4.append( _currTime4.getTimeInMillis() ) ;
		
		Resource _friend4 = _model.createResource( TwitterConstants.TWITTER_USER_URI + "654321" ) ;
		Statement _s4 = _model.createStatement( _subject, TWITTER.Has_Friend, _friend4 ) ; _model.add( _s4 ) ;
		ReifiedStatement _rs4 = _model.createReifiedStatement( _sb4.toString(), _s4 ) ;
		_model.add( _rs4, STORMRIDER.Timestamp, _model.createTypedLiteral( _currTime4 ) ) ;
		_s4 = null ; _rs4 = null ;
		
		Thread.sleep( 10000 ) ;
		
		Calendar _currTime5 = Calendar.getInstance() ; 
		StringBuilder _sb5 = new StringBuilder() ; _sb5.append( _model.getNsPrefixURI( StormRiderConstants.REIFIED_STATEMENT_NS ) ) ; _sb5.append( "123456" ) ; _sb5.append( "-" ) ; _sb5.append( _currTime5.getTimeInMillis() ) ;
		
		Resource _friend5 = _model.createResource( TwitterConstants.TWITTER_USER_URI + "888888" ) ;
		Statement _s5 = _model.createStatement( _subject, TWITTER.Has_Friend, _friend5 ) ; _model.add( _s5 ) ;
		ReifiedStatement _rs5 = _model.createReifiedStatement( _sb5.toString(), _s5 ) ;
		_model.add( _rs5, STORMRIDER.Timestamp, _model.createTypedLiteral( _currTime5 ) ) ;
		_s5 = null ; _rs5 = null ;


		System.out.println( "*********************All statements********************" ) ;
		StmtIterator _iterStmts = _model.listStatements() ;
		while( _iterStmts.hasNext() )
			System.out.println( _iterStmts.next().toString() ) ;
		System.out.println( "*******************************************************" ) ;

		System.out.println( "***Friends on or before " + _currTime2.getTime().toString() + "***" ) ;
		String qs1 = 
		" PREFIX ex:  <http://www.example.org/people#u-> " +
		" PREFIX tw:  <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" PREFIX sr:  <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/stormrider/0.1#> " +
		" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
		" PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
		" SELECT ?z " +
		" WHERE " + 
		" { " +
		" 		?x sr:Timestamp ?y . " + 
		" 		?x rdf:subject ex:123456 . " +
		" 		?x rdf:predicate tw:Has_Friend . " +
		" 		?x rdf:object ?z . " +
		" 		FILTER( ?y <= \"" + _model.createTypedLiteral( _currTime2 ).getValue() + "\"^^xsd:dateTime ) " +
		" } " ; 
		
        QueryExecution qexec1 = QueryExecutionFactory.create( qs1, _model ) ;
        ResultSet rs1 = qexec1.execSelect() ;
        while( rs1.hasNext() )
        {
                QuerySolution rb = rs1.nextSolution() ;
                Resource fr = rb.getResource( "?z" ) ;
                System.out.println( "A friend: " + fr.toString() ) ;
        }
        qexec1.close();
		System.out.println( "*******************************************************" ) ;
		
		System.out.println( "***Friends on or before " + _currTime4.getTime().toString() + "***" ) ;
		String qs2 = 
		" PREFIX ex:  <http://www.example.org/people#u-> " +
		" PREFIX tw:  <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
		" PREFIX sr:  <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/stormrider/0.1#> " +
		" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
		" PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
		" SELECT DISTINCT ?z " +
		" WHERE " + 
		" { " +
		" 		?x sr:Timestamp ?y . " + 
		" 		?x rdf:subject ex:123456 . " +
		" 		?x rdf:predicate tw:Has_Friend . " +
		" 		?x rdf:object ?z . " +
		" 		FILTER( ?y <= \"" + _model.createTypedLiteral( _currTime4 ).getValue() + "\"^^xsd:dateTime ) " +
		" } " ; 
		
        QueryExecution qexec2 = QueryExecutionFactory.create( qs2, _model ) ;
        ResultSet rs2 = qexec2.execSelect() ;
        while( rs2.hasNext() )
        {
                QuerySolution rb = rs2.nextSolution() ;
                Resource fr = rb.getResource( "?z" ) ;
                System.out.println( "A friend: " + fr.toString() ) ;
        }
        qexec2.close();
		System.out.println( "*******************************************************" ) ;
	}
}