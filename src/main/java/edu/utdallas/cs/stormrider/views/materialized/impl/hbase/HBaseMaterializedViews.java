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

import edu.utdallas.cs.stormrider.connection.StormRiderConnection;
import edu.utdallas.cs.stormrider.views.materialized.MaterializedViewsBase;
import edu.utdallas.cs.stormrider.views.materialized.MaterializedViewsDesc;
import edu.utdallas.cs.stormrider.views.materialized.MaterializedViewsException;

public class HBaseMaterializedViews extends MaterializedViewsBase
{
    /** The friends adjacency list view as a HTable **/
    protected HBaseNodesView nodesView = null ;

    /** The landmarks with their assoicated information as a HTable **/
    protected HBaseLandmarksView landmarksView = null ;

	public HBaseMaterializedViews( StormRiderConnection conn, MaterializedViewsDesc desc )
	{
		super( conn, desc ) ;
        try
        {
        	nodesView = new HBaseNodesView( linkName, conn ) ;
        	landmarksView = new HBaseLandmarksView( linkName, conn ) ;
        }
        catch( Exception e ) { throw new MaterializedViewsException( "Exception during view construction in views materialized in HBase:: ", e ) ; }
	}    
	
    public HBaseNodesView getNodesView() { return nodesView ; }
    
    public HBaseLandmarksView getLandmarksView() { return landmarksView ; }
}