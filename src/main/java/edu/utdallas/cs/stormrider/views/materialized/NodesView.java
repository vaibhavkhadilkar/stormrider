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

package edu.utdallas.cs.stormrider.views.materialized;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import edu.utdallas.cs.stormrider.connection.StormRiderConnection;

public interface NodesView 
{
	public void updateMetricValue( String row, String metric, double value ) ;
	
	public long getTotalNodes() ;
	
	public Iterator<Result> getAllRows() ;

	public String getClosestLandmark( String row ) ;

	public Iterator<byte[]> getAdjList( String row ) ;
	
	public String getLinkName() ;
	
	public StormRiderConnection getConnection() ;
	
	public void updateAdjacencyListValue( String row, String adjList ) ;
	
	public boolean getIsLandmark( String row ) ;
	
	public void updateIsLandmarkValue( String row, String value ) ;
	
	public void updateDistToClosestLandmarkValue( String row, String value ) ;
	
	public long getDistToClosestLandmark( String row ) ;
	
	public void updateClosestLandmarkValue( String row, String value ) ;
	
	public List<String> getNonLandmarksList() ;
}