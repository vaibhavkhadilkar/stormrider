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

package edu.utdallas.cs.stormrider.views.materialized;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import edu.utdallas.cs.stormrider.connection.StormRiderConnection;

public interface LandmarksView 
{
	public long getDistanceToLandmark( String row ) ;

	public long getNumOfPathsToLandmark( String row ) ;

	public List<String> getListOfPathsToLandmark( String row ) ;

	public Iterator<Result> getRowsForLandmark( String landmark ) ;
	
	public String getLinkName() ;
	
	public StormRiderConnection getConnection() ;
	
	public void updateDistanceValue( String row, String value ) ;
	
	public void updateNumOfPathsValue( String row, String value ) ;
	
	public void updatePathsValue( String row, String value ) ;
}
