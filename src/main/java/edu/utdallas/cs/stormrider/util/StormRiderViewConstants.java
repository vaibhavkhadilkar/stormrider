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

public class StormRiderViewConstants 
{	
    /** A suffix for the nodes view HTable **/
    public static final String nodesViewSuffix = "-NodesView" ;
    
    /** The list of column families in the nodes view **/
    public static final String colFamAdjList = "AdjList", colFamMetric = "Metric", colFamLandmark = "Landmark" ;

    public static final String colMetricDegC = "DegC", colMetricCloseC = "CloseC", colMetricBetC = "BetC" ;
    
    public static final String colLandmarkIsLandmark = "Is-Landmark", colLandmarkDistToClosestLandmark = "Dist-To-Closest-Landmark", colLandmarkClosestLandmark = "Closest-Landmark" ;

    public static final String colNodesDistance = "Distance", colNodesNumOfPaths = "Num-Of-Paths", colNodesPaths = "Paths" ;
    
    public static final String cellDelimiter = "&" ;
    
    /** A suffix for the landmarks view HTable **/
    public static final String landmarksViewSuffix = "-LandmarksView" ;

    /** The column families of the landmarks view **/
    public static final String colFamNode = "Node" ;    
}
