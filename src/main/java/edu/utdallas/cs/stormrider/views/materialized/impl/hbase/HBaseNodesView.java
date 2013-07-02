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

package edu.utdallas.cs.stormrider.views.materialized.impl.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import edu.utdallas.cs.stormrider.connection.StormRiderConnection;
import edu.utdallas.cs.stormrider.util.StormRiderViewConstants;
import edu.utdallas.cs.stormrider.views.materialized.MaterializedViewsException;
import edu.utdallas.cs.stormrider.views.materialized.NodesViewBase;

/**
 * A class that handles various functions for the nodes view
 */
public class HBaseNodesView extends NodesViewBase
{	
	/** The adjacency list view for a given link name **/
	private HTable nodesTable = null;

	public HBaseNodesView( String lName, StormRiderConnection conn ) throws IOException
	{
		super( lName, conn ) ;
		constructNodesTable() ;
	}
	
	private void constructNodesTable() throws IOException
	{
		String viewName = linkName + StormRiderViewConstants.nodesViewSuffix ;
		
		if( conn.doesTableExist( viewName ) )
			nodesTable = conn.openTable( viewName ) ;
		else
		{
			List<String> cols = new ArrayList<String>() ;
			cols.add( StormRiderViewConstants.colFamAdjList ) ;
			cols.add( StormRiderViewConstants.colFamMetric ) ;
			cols.add( StormRiderViewConstants.colFamLandmark ) ;
			nodesTable = conn.createTable( viewName, cols ) ;
		}		
	}

	public List<String> getNonLandmarksList()
	{
		List<String> nonLandmarksList = new ArrayList<String>() ;
		
		return nonLandmarksList ;
	}
	
	public void updateClosestLandmarkValue( String row, String value )
	{
		try
		{
			byte[] rowBytes = Bytes.toBytes( row ), colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamLandmark ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colLandmarkClosestLandmark ), val = Bytes.toBytes( value ) ;
			Put update = new Put( rowBytes ) ;
			update.add( colFamilyBytes, colQualBytes, val ) ;
			nodesTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, val, update ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during updation of closest landmark value in NodesView:: ", e ) ; }		
	}

	public void updateDistToClosestLandmarkValue( String row, String value )
	{
		try
		{
			byte[] rowBytes = Bytes.toBytes( row ), colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamLandmark ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colLandmarkDistToClosestLandmark ), val = Bytes.toBytes( value ) ;
			Put update = new Put( rowBytes ) ;
			update.add( colFamilyBytes, colQualBytes, val ) ;
			nodesTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, val, update ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during updation of dist to closest landmark value in NodesView:: ", e ) ; }		
	}

	public void updateIsLandmarkValue( String row, String value )
	{
		try
		{
			byte[] rowBytes = Bytes.toBytes( row ), colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamLandmark ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colLandmarkIsLandmark ), val = Bytes.toBytes( value ) ;
			Put update = new Put( rowBytes ) ;
			update.add( colFamilyBytes, colQualBytes, val ) ;
			nodesTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, val, update ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during updation of is landmark value in NodesView:: ", e ) ; }		
	}

	public boolean getIsLandmark( String row )
	{
		boolean isLandmark = false ;
		try
		{
			byte[] colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamLandmark ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colLandmarkIsLandmark ) ;
			Get res = new Get( Bytes.toBytes( row ) ) ; res.addColumn( colFamilyBytes, colQualBytes ) ;
			isLandmark = ( Bytes.toString( nodesTable.get( res ).value() ) == "Y" ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during check of whether given node is a landmark in NodesView:: ", e ) ; }
		return isLandmark ;
	}

	public long getDistToClosestLandmark( String row )
	{
		long distToClosestLandmark = 0L ;
		try
		{
			byte[] colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamLandmark ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colLandmarkDistToClosestLandmark ) ;
			Get res = new Get( Bytes.toBytes( row ) ) ; res.addColumn( colFamilyBytes, colQualBytes ) ;
			distToClosestLandmark = Bytes.toLong( nodesTable.get( res ).value() ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of dist to closest landmark in NodesView:: ", e ) ; }
		return distToClosestLandmark ;
	}

	
	public void updateAdjacencyListValue( String row, String adjList )
	{
		try
		{
			byte[] rowBytes = Bytes.toBytes( row ), colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamAdjList ), colQualBytes = Bytes.toBytes( adjList ) ;
			Put update = new Put( rowBytes ) ;
			update.add( colFamilyBytes, colQualBytes, Bytes.toBytes( "" ) ) ;
			nodesTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, null, update ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during updation of adj list in NodesView:: ", e ) ; }		
	}
	
	public void updateMetricValue( String row, String metric, double value )
	{
		try
		{
			byte[] rowBytes = Bytes.toBytes( row ), colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamMetric ), colQualBytes = Bytes.toBytes( metric ), val = Bytes.toBytes( value ) ;
			Put update = new Put( rowBytes ) ;
			update.add( colFamilyBytes, colQualBytes, val ) ;
			nodesTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, val, update ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during updation of metric value in NodesView:: ", e ) ; }
	}
	
	public long getTotalNodes()
	{
		long totalNodes = 0L ;
		try
		{
			Iterator<Result> iterTable = getAllRows() ;
			while( iterTable.hasNext() )
			{
				totalNodes++ ; 
				iterTable.next() ;
			}
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of total nodes in NodesView:: ", e ) ; }
		return totalNodes ;
	}
	
	public Iterator<Result> getAllRows()
	{
		Iterator<Result> iterTable = null ;
		try
		{
			Scan scanner = new Scan() ;
			iterTable = nodesTable.getScanner( scanner ).iterator() ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of all rows in NodesView:: ", e ) ; }
		return iterTable ;
	}
	
	public String getClosestLandmark( String row )
	{
		String closestLandmark = "" ;
		try
		{
			byte[] colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamLandmark ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colLandmarkClosestLandmark ) ;
			Get res = new Get( Bytes.toBytes( row ) ) ; res.addColumn( colFamilyBytes, colQualBytes ) ;
			closestLandmark = Bytes.toString( nodesTable.get( res ).value() ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of closest landmark in NodesView:: ", e ) ; }
		return closestLandmark ;
	}
	
	public Iterator<byte[]> getAdjList( String row )
	{
		Iterator<byte[]> iterAdjList = null ;
		try
		{
			Get res = new Get( Bytes.toBytes( row ) ) ;
			iterAdjList = nodesTable.get( res ).getFamilyMap( StormRiderViewConstants.colFamAdjList.getBytes() ).keySet().iterator() ;
		}
        catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of adj. list in NodesView:: ", e ) ; }
        return iterAdjList ;
	}
}