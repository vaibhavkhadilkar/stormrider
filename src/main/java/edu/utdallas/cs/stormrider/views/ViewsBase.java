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

package edu.utdallas.cs.stormrider.views;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import edu.utdallas.cs.stormrider.views.materialized.LandmarksView;
import edu.utdallas.cs.stormrider.views.materialized.MaterializedViews;
import edu.utdallas.cs.stormrider.views.materialized.NodesView;

public abstract class ViewsBase implements Views
{
	protected MaterializedViews mvs = null ;
	
	protected NodesView nView = null ;
	
	protected LandmarksView lView = null ;
	
	public ViewsBase( MaterializedViews mvs )
	{
		this.mvs = mvs ;
		this.nView = mvs.getNodesView() ;
		this.lView = mvs.getLandmarksView() ;
	}
	
	public long getTotalNodes() { return nView.getTotalNodes() ; }
	
	public Iterator<byte[]> getAdjList( String row ) { return nView.getAdjList( row ) ; }
	
	public void updateMetricValue( String row, String metric, double value ) { nView.updateMetricValue( row, metric, value ) ; }

	public Iterator<Result> getAllRows() { return nView.getAllRows() ; }
	
	public String getClosestLandmark( String row ) { return nView.getClosestLandmark( row ) ; }
	
	public long getDistanceToLandmark( String row ) { return lView.getDistanceToLandmark( row ) ; }

	public long getNumOfPathsToLandmark( String row ) { return lView.getNumOfPathsToLandmark( row ) ; }

	public List<String> getListOfPathsToLandmark( String row ) { return lView.getListOfPathsToLandmark( row ) ; }

	public Iterator<Result> getRowsForLandmark( String landmark ) { return lView.getRowsForLandmark( landmark ) ; }
	
	public String getLinkNameAsURI() { return mvs.getDesc().getLinkName() ; }
	
	public void updateAdjacencyListValue( String row, String adjList ) { nView.updateAdjacencyListValue( row, adjList ) ; }
	
	public boolean getIsLandmark( String row ) { return nView.getIsLandmark( row ) ; }
	
	public void updateIsLandmarkValue( String row, String value ) { nView.updateIsLandmarkValue( row, value ) ; }
	
	public void updateDistToClosestLandmarkValue( String row, String value ) { nView.updateDistToClosestLandmarkValue( row, value ) ; }
	
	public long getDistToClosestLandmark( String row ) { return nView.getDistToClosestLandmark( row ) ; }
	
	public void updateClosestLandmarkValue( String row, String value ) { nView.updateClosestLandmarkValue( row, value ) ; } 
	
	public List<String> getNonLandmarksList() { return nView.getNonLandmarksList() ; }
	
	public void updateDistanceValue( String row, String value ) { lView.updateDistanceValue( row, value ) ; }
	
	public void updateNumOfPathsValue( String row, String value ) { lView.updateNumOfPathsValue( row, value ) ; }
	
	public void updatePathsValue( String row, String value ) { lView.updatePathsValue( row, value ) ; }
}