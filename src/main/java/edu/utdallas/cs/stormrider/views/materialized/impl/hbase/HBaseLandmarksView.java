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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import edu.utdallas.cs.stormrider.connection.StormRiderConnection;
import edu.utdallas.cs.stormrider.util.StormRiderViewConstants;
import edu.utdallas.cs.stormrider.views.materialized.LandmarksViewBase;
import edu.utdallas.cs.stormrider.views.materialized.MaterializedViewsException;

public class HBaseLandmarksView extends LandmarksViewBase
{
	/** The landmarks HTable for a given link name **/
	private HTable landmarksTable = null;
	
	public HBaseLandmarksView( String lName, StormRiderConnection conn ) throws IOException
	{
		super( lName, conn ) ;
		constructLandmarksTable() ;
	}
	
	private void constructLandmarksTable() throws IOException
	{
		String viewName = linkName + StormRiderViewConstants.landmarksViewSuffix ;
		
		if( conn.doesTableExist( viewName ) )
			landmarksTable = conn.openTable( viewName ) ;
		else
		{
			List<String> cols = new ArrayList<String>() ;
			cols.add( StormRiderViewConstants.colFamNode ) ;
			landmarksTable = conn.createTable( viewName, cols ) ;
		}		
	}

	public String getCurrentPaths( String row )
	{
		String listOfPaths = null ;
		try
		{
			byte[] colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamNode ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colNodesPaths ) ;
			Get res = new Get( Bytes.toBytes( row ) ) ; res.addColumn( colFamilyBytes, colQualBytes ) ;
			listOfPaths = Bytes.toString( landmarksTable.get( res ).value() ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of num of paths to landmark in LandmarksView:: ", e ) ; }
		return listOfPaths ;
	}	

	public void updatePathsValue( String row, String value )
	{
		try
		{
			StringBuilder sbVal = new StringBuilder() ; sbVal.append( getCurrentPaths( row ) ) ; sbVal.append( StormRiderViewConstants.cellDelimiter ) ; sbVal.append( value ) ;
			byte[] rowBytes = Bytes.toBytes( row ), colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamNode ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colNodesPaths ), val = Bytes.toBytes( sbVal.toString() ) ;
			Put update = new Put( rowBytes ) ;
			update.add( colFamilyBytes, colQualBytes, val ) ;
			landmarksTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, val, update ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during updation of number of paths value in LandmarksView:: ", e ) ; }				
	}

	public void updateNumOfPathsValue( String row, String value )
	{
		try
		{
			byte[] rowBytes = Bytes.toBytes( row ), colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamNode ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colNodesNumOfPaths ), val = Bytes.toBytes( value ) ;
			Put update = new Put( rowBytes ) ;
			update.add( colFamilyBytes, colQualBytes, val ) ;
			landmarksTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, val, update ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during updation of number of paths value in LandmarksView:: ", e ) ; }				
	}
	
	public void updateDistanceValue( String row, String value )
	{
		try
		{
			byte[] rowBytes = Bytes.toBytes( row ), colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamNode ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colNodesDistance ), val = Bytes.toBytes( value ) ;
			Put update = new Put( rowBytes ) ;
			update.add( colFamilyBytes, colQualBytes, val ) ;
			landmarksTable.checkAndPut( rowBytes, colFamilyBytes, colQualBytes, val, update ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during updation of distance value in LandmarksView:: ", e ) ; }		
	}

	public long getDistanceToLandmark( String row )
	{
		long distance = 0L ;
		try
		{
			byte[] colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamNode ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colNodesDistance ) ;
			Get res = new Get( Bytes.toBytes( row ) ) ; res.addColumn( colFamilyBytes, colQualBytes ) ;
			distance = Bytes.toLong( landmarksTable.get( res ).value() ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of distance to landmark in LandmarksView:: ", e ) ; }
		return distance ;
	}
	
	public long getNumOfPathsToLandmark( String row )
	{
		long numOfPaths = 0L ;
		try
		{
			byte[] colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamNode ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colNodesNumOfPaths ) ;
			Get res = new Get( Bytes.toBytes( row ) ) ; res.addColumn( colFamilyBytes, colQualBytes ) ;
			numOfPaths = Bytes.toLong( landmarksTable.get( res ).value() ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of num of paths to landmark in LandmarksView:: ", e ) ; }
		return numOfPaths ;
	}	

	public List<String> getListOfPathsToLandmark( String row )
	{
		List<String> listOfPaths = null ;
		try
		{
			byte[] colFamilyBytes = Bytes.toBytes( StormRiderViewConstants.colFamNode ), colQualBytes = Bytes.toBytes( StormRiderViewConstants.colNodesPaths ) ;
			Get res = new Get( Bytes.toBytes( row ) ) ; res.addColumn( colFamilyBytes, colQualBytes ) ;
			listOfPaths = Arrays.asList( Bytes.toString( landmarksTable.get( res ).value() ).split( StormRiderViewConstants.cellDelimiter ) ) ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of num of paths to landmark in LandmarksView:: ", e ) ; }
		return listOfPaths ;
	}	

	public Iterator<Result> getRowsForLandmark( String landmark )
	{
		Iterator<Result> iterRowsForLandmark = null ;
		try
		{
			Scan scanner = new Scan( landmark.getBytes() ) ;
			scanner.setFilter( new RowFilter( CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator( landmark.getBytes() ) ) ) ;
			iterRowsForLandmark = landmarksTable.getScanner( scanner ).iterator() ;
		}
		catch( Exception e ) { throw new MaterializedViewsException( "Exception during retrieval of rows for given landmark in LandmarksView:: ", e ) ; }
		return iterRowsForLandmark ;
	}
}