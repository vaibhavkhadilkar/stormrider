package edu.utdallas.cs.stormrider.examples.twitter;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;

import edu.utdallas.cs.stormrider.util.TwitterConstants;

public class TwitterStoreTest 
{
	public static void main( String[] args )
	{
		Store store = HBaseRdfFactory.connectStore( "conf/twitter-model-sample.ttl" ) ;
		Model model = HBaseRdfFactory.connectNamedModel( store, TwitterConstants.IRI ) ;
		
		String queryString =
			" PREFIX twitter: <http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#> " +
			" SELECT ?x ?y " +
			" WHERE { ?x twitter:Screen_Name ?y } " ;
		QueryExecution qe = QueryExecutionFactory.create( queryString, model );
		ResultSet rs = qe.execSelect();
		ResultSetFormatter.out( rs );
		
		StmtIterator iter = model.listStatements();
		while( iter.hasNext() ) { System.out.println( iter.next().toString() ); }	
	}
}