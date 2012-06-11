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

/**
 * A class that holds constants for Twitter attributes
 */
public class TwitterConstants
{
	/** The consumer key for OAuth support **/
	public static final String CONSUMER_KEY = "<== Twitter consumer key ==>" ;
	
	/** The consumer secret for OAuth support **/
	public static final String CONSUMER_SECRET = "<== Twitter consumer secret ==>" ;
	
	/** The access token for OAuth support **/
	public static final String ACCESS_TOKEN = "<== Twitter access token ==>" ;
		
	/** The access token secret for OAuth support **/
	public static final String ACCESS_TOKEN_SECRET = "<== Twitter access token secret ==>" ;
	
	/** The URI for Twitter users **/
	public static final String TWITTER_USER_URI = "http://www.example.org/people#u-" ;
	
	/** The URI for Twitter tweets **/
	public static final String TWITTER_TWEET_URI = "http://www.example.org/messages#t-" ;
	
	/** The number of ms to wait in between requests to the Twitter API **/
	public static final long TIME_DELAY_BETWEEN_REQUESTS = 2000L ;
	
	/** The number of status messageds to retrieve per page **/
	public static final int STATUSES_PER_PAGE = 100 ;	
	
	/** The location of the HBase configuration file **/
	public static final String HBASE_MODEL_CONFIG_FILE = "<== location of jena-hbase config file ==>" ;

	/** The location of the HBase configuration file **/
	public static final String HBASE_VIEW_CONFIG_FILE = "<== location of view config file ==>" ;

	public static final String STORAGE_TOPOLOGY_NAME = "storage-topology" ;

	public static final int NUM_OF_WORKERS = 14 ;	
	
	public static final int NUM_OF_TASKS = NUM_OF_WORKERS * 4 ;
	
	public static final boolean IS_DISTRIBUTED = false ;
	
	public static final boolean IS_LOCAL_MODE = true ;
	
	public static final long MAX_REPORTS = 1000 ;
	
	public static final long INTERVAL = 3600 ;
	
	public static String HBASE_CONFIG_FILE = "<== location of hbase-site.xml ==>" ;
}