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

package edu.utdallas.cs.stormrider.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class GeneralUtil 
{
	private static Configuration hbaseConfig = null ;
	
    /**
     * A method that returns the singleton HBase Configuration object
     * @return a Configuration object
     */
    public static Configuration getHBaseConfig()
    {
    	if( hbaseConfig == null )
        {
    		hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.addResource( new Path( StormRiderViewConstants.hbaseConfigFile ) );
        }
        return hbaseConfig;
    }
}
