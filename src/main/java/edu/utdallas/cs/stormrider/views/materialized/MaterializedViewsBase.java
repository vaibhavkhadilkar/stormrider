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

import edu.utdallas.cs.stormrider.connection.StormRiderConnection;

public abstract class MaterializedViewsBase implements MaterializedViews
{
    /** The link name for which the views are being created **/
    protected String linkName = null ;

    protected StormRiderConnection conn = null ;
    
    protected MaterializedViewsDesc desc = null ;
    
    public MaterializedViewsBase( StormRiderConnection conn, MaterializedViewsDesc desc )
    {
    	this.conn = conn ;
    	this.desc = desc ;
		this.linkName = desc.getLinkName().split( "#" )[1] ;
    }
    
    public StormRiderConnection getConnection() { return conn ; }
    
    public MaterializedViewsDesc getDesc() { return desc ; }
    
    public String getLinkName() { return linkName ; }
}
