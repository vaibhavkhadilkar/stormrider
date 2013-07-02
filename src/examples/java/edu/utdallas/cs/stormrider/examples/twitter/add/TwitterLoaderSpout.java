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

package edu.utdallas.cs.stormrider.examples.twitter.add;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.VCARD;

import edu.utdallas.cs.stormrider.store.Store;
import edu.utdallas.cs.stormrider.store.StoreFactory;
import edu.utdallas.cs.stormrider.store.iterator.impl.NodeAndNeighbor;
import edu.utdallas.cs.stormrider.util.TwitterConstants;
import edu.utdallas.cs.stormrider.views.Views;
import edu.utdallas.cs.stormrider.views.ViewsFactory;
import edu.utdallas.cs.stormrider.vocab.TWITTER;

import twitter4j.IDs;
import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A Spout for emitting tuples from the Twitter data stream
 */
public class TwitterLoaderSpout implements IRichSpout 
{
	/** A default serial version uid **/
	private static final long serialVersionUID = 1L ;

	/** A Logger for this class **/
    private static Logger LOG = Logger.getLogger( TwitterLoaderSpout.class ) ;
    
    /** An output collector used to emit tuples from the Twitter stream **/
    private SpoutOutputCollector collector ;

    /** The OAuth verified Twitter handle **/
    private Twitter twitter = null ;

    private boolean isReified = false ;
    
    private boolean formatStore = false ;
    
    private String storeConfigFile = null ;
    
    private String iri = null ;
    
    private String viewsConfigFile = null ;
    
    /** Constructor **/
    public TwitterLoaderSpout( boolean isReified, String storeConfigFile, String viewsConfigFile ) 
    { this( isReified, storeConfigFile, viewsConfigFile, "" ) ; }

    /** Constructor **/
    public TwitterLoaderSpout( boolean isReified, String storeConfigFile, String viewsConfigFile, String iri ) 
    { this( isReified, false, storeConfigFile, viewsConfigFile, iri ) ; }

    /** Constructor **/
    public TwitterLoaderSpout( boolean isReified, boolean formatStore, String storeConfigFile, String viewsConfigFile, String iri ) 
    { 
		twitter = new TwitterFactory().getInstance() ;
		twitter.setOAuthConsumer( TwitterConstants.CONSUMER_KEY, TwitterConstants.CONSUMER_SECRET ) ;
		AccessToken atoken = new AccessToken( TwitterConstants.ACCESS_TOKEN, TwitterConstants.ACCESS_TOKEN_SECRET ) ;
		twitter.setOAuthAccessToken( atoken ) ;
		this.isReified = isReified ;
		this.formatStore = formatStore ;
		this.storeConfigFile = storeConfigFile ;
		this.iri = iri ;
		this.viewsConfigFile = viewsConfigFile ;
		if( formatStore ) StoreFactory.getJenaHBaseStore( storeConfigFile, iri, isReified, formatStore ) ;
    }
    
	@SuppressWarnings("rawtypes")
	@Override
    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) { this.collector = collector ; }

    @Override
    public void close() { }
    
    @Override
    public void nextTuple() 
    {
		Store store = StoreFactory.getJenaHBaseStore( storeConfigFile, iri, isReified, false ) ;
		Views views = ViewsFactory.getViews( viewsConfigFile ) ;

    	Random random = new Random() ;
    	try
    	{
   			//Emit user-specific information
   			int randomUserId = random.nextInt( 400000000 ) ;
   			emitUserAndTweetInformation( randomUserId ) ;    			
   			Thread.sleep( 100000 ) ;
    			
/*    			//Emit user-id for updating node-centric view
    			String tupleType = "nv" ;
    			StringBuilder subject = new StringBuilder( "u-" ) ; subject.append( randomUserId ) ;
    			collector.emit( new Values( tupleType, subject.toString(), "", "" ) ) ;
    			
    			//Emit info for updating landmarks-centric view
       			tupleType = "nlv" ;
       			Iterator<NodeAndNeighbor> iterNodesAndNeighbors = store.getAllNodesWithNeighbors( views.getLinkNameAsURI() ) ;
       			while( iterNodesAndNeighbors.hasNext() )
       			{
       				NodeAndNeighbor nodeAndNeighbor = iterNodesAndNeighbors.next() ;
       				collector.emit( new Values( tupleType, nodeAndNeighbor.getNode(), "", "" ) ) ;
       			}
*/
    	}
    	catch( Exception e ) 
    	{
    		if( e.toString().contains( "403" ) || e.toString().contains( "404" ) )
    			LOG.info( "User account suspended or deleted or url not found" ) ;
    		else
    			LOG.info( "Error in retrieving user information", e ) ; 
    	}
    }
    
    @Override
    public void ack( Object msgId ) { }

    @Override
    public void fail( Object msgId ) { }
    
    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) { declarer.declare( new Fields( "tupleType", "node1", "node2", "node3" ) ) ; }
    
	@Override
	public void activate() { }

	@Override
	public void deactivate() { }

	@Override
	public Map<String, Object> getComponentConfiguration() { return null ; }

    private void emitUserAndTweetInformation( int randomUserId ) throws TwitterException, InterruptedException
    {
    	//Define a triple tuple type, since we emit triples in this method
    	String tupleType = "triple" ;
    	
		//Get user-specific information
		User user = twitter.showUser( randomUserId ) ;
		
		StringBuilder subject = new StringBuilder( TwitterConstants.TWITTER_USER_URI ) ; subject.append( user.getId() ) ;
		
		LOG.info( subject.toString() ) ;
		
		StringBuilder userName = new StringBuilder( "lit~" ) ; if( user.getName() != null ) userName.append( user.getName() ) ; 									
		if( !userName.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), VCARD.NAME.toString(), userName.toString() ) ) ;
		userName = null ;
		
		StringBuilder userProfileImageUrl = new StringBuilder( "res~" ) ; if( user.getProfileBackgroundImageURL() != null ) userProfileImageUrl.append( user.getProfileImageURL() ) ; 	
		if( !userProfileImageUrl.toString().equals( "res~" ) ) collector.emit( new Values( tupleType, subject.toString(), VCARD.PHOTO.toString(), userProfileImageUrl.toString() ) ) ;
		userProfileImageUrl = null ;
		
		StringBuilder userUrl = new StringBuilder( "res~" ) ; if( user.getURL() != null ) userUrl.append( user.getURL() ) ; 							
		if( !userUrl.toString().equals( "res~" ) ) collector.emit( new Values( tupleType, subject.toString(), FOAF.homepage.toString(), userUrl.toString() ) ) ;
		userUrl = null ;
		
		StringBuilder userTZ = new StringBuilder( "lit~" ) ; if( user.getTimeZone() != null ) userTZ.append( user.getTimeZone() ) ; 									
		if( !userTZ.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), VCARD.TZ.toString(), userTZ.toString() ) ) ;
		userTZ = null ;
		
		StringBuilder userScreenName = new StringBuilder( "res~" ) ; if( user.getScreenName() != null ) { userScreenName.append( TwitterConstants.TWITTER_USER_URI ) ; userScreenName.append( user.getScreenName() ) ; }  						
		if( !userScreenName.toString().equals( "res~" ) )
		{
			collector.emit( new Values( tupleType, subject.toString(), TWITTER.Screen_Name.toString(), userScreenName.toString() ) ) ;
			collector.emit( new Values( tupleType, subject.toString(), OWL.sameAs.toString(), userScreenName.toString() ) ) ;
		}
		userScreenName = null ;
		
		StringBuilder userLocation = new StringBuilder( "lit~" ) ; if( user.getLocation() != null ) userLocation.append( user.getLocation() ) ; 							
		if( !userLocation.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Location.toString(), userLocation.toString() ) ) ;
		userLocation = null ;
		
		StringBuilder userDescription = new StringBuilder( "lit~" ) ; if( user.getDescription() != null ) userDescription.append( user.getDescription() ) ; 						
		if( !userDescription.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Description.toString(), userDescription.toString() ) ) ;
		userDescription = null ;
		
		StringBuilder userFollowersCount = new StringBuilder( "lit~" ) ; if( user.getFollowersCount() > 0 ) userFollowersCount.append( user.getFollowersCount() ) ;
		if( !userFollowersCount.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Followers_Count.toString(), userFollowersCount.toString() ) ) ;
		userFollowersCount = null ;
		
		StringBuilder userFriendsCount = new StringBuilder( "lit~" ) ; if( user.getFriendsCount() > 0 ) userFriendsCount.append( user.getFriendsCount() ) ;
		if( !userFriendsCount.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Friends_Count.toString(), userFriendsCount.toString() ) ) ;
		userFriendsCount = null ;
		
		Date userCreatedAt = user.getCreatedAt() ;
		StringBuilder userCreated = new StringBuilder( "lit~" ) ; userCreated.append( userCreatedAt.toString() ) ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Created_At.toString(), userCreated.toString() ) ) ;
		userCreated = null ; userCreatedAt = null ;
		
		StringBuilder userFavouritesCount = new StringBuilder( "lit~" ) ; if( user.getFavouritesCount() > 0 ) userFavouritesCount.append( user.getFavouritesCount() ) ;
		if( !userFavouritesCount.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Favourites_Count.toString(), userFavouritesCount.toString() ) ) ;
		userFavouritesCount = null ;
		
		StringBuilder userStatusesCount = new StringBuilder( "lit~" ) ; if( user.getStatusesCount() > 0 ) userStatusesCount.append( user.getStatusesCount() ) ;
		if( !userStatusesCount.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Statuses_Count.toString(), userStatusesCount.toString() ) ) ;
		userStatusesCount = null ;
		
		StringBuilder userUtcOffset = new StringBuilder( "lit~" ) ; if( user.getUtcOffset() > 0 ) userUtcOffset.append( user.getUtcOffset() ) ;
		if( !userUtcOffset.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.UTC_Offset.toString(), userUtcOffset.toString() ) ) ;
		userUtcOffset = null ;
		
		StringBuilder userProfileBGColor = new StringBuilder( "lit~" ) ; if( user.getProfileBackgroundColor() != null ) userProfileBGColor.append( user.getProfileBackgroundColor() ) ; 		
		if( !userProfileBGColor.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Profile_Background_Color.toString(), userProfileBGColor.toString() ) ) ;
		userProfileBGColor = null ;
		
		StringBuilder userProfileTextColor = new StringBuilder( "lit~" ) ; if( user.getProfileTextColor() != null ) userProfileTextColor.append( user.getProfileTextColor() ) ; 			
		if( !userProfileTextColor.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Profile_Text_Color.toString(), userProfileTextColor.toString() ) ) ;
		userProfileTextColor = null ;
		
		StringBuilder userProfileLinkColor = new StringBuilder( "lit~" ) ; if( user.getProfileLinkColor() != null ) userProfileLinkColor.append( user.getProfileLinkColor() ) ; 			
		if( !userProfileLinkColor.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Profile_Link_Color.toString(), userProfileLinkColor.toString() ) ) ;
		userProfileLinkColor = null ;
		
		StringBuilder userProfileSFillColor = new StringBuilder( "lit~" ) ; if( user.getProfileSidebarFillColor() != null ) userProfileSFillColor.append( user.getProfileSidebarFillColor() ) ; 	
		if( !userProfileSFillColor.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Profile_Sidebar_Fill_Color.toString(), userProfileSFillColor.toString() ) ) ;
		userProfileSFillColor = null ;
		
		StringBuilder userProfileSBorderColor = new StringBuilder( "lit~" ) ; if( user.getProfileSidebarBorderColor() != null ) userProfileSBorderColor.append( user.getProfileSidebarBorderColor() ) ;
		if( !userProfileSBorderColor.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Profile_Sidebar_Border_Color.toString(), userProfileSBorderColor.toString() ) ) ;
		userProfileSBorderColor = null ;
		
		StringBuilder userLang = new StringBuilder( "lit~" ) ; if( user.getLang() != null ) userLang.append( user.getLang() ) ;								
		if( !userLang.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Lang.toString(), userLang.toString() ) ) ;
		userLang = null ;
		
		StringBuilder userListedCount = new StringBuilder( "lit~" ) ; if( user.getListedCount() > 0 ) userListedCount.append( user.getListedCount() ) ;								
		if( !userListedCount.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, subject.toString(), TWITTER.Listed_Count.toString(), userListedCount.toString() ) ) ; 
		
		boolean isProfileBGTiled = user.isProfileBackgroundTiled() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Profile_Background_Tiled.toString(), "" + isProfileBGTiled ) ) ;
		
		boolean isProfileBGImageUsed = user.isProfileUseBackgroundImage() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Profile_Background_Image_Used.toString(), "" + isProfileBGImageUsed ) ) ;
		
		boolean isProtected = user.isProtected() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Protected.toString(), "" + isProtected ) ) ;
		
		boolean isGeoEnabled = user.isGeoEnabled() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Geo_Enabled.toString(), "" + isGeoEnabled ) ) ;
		
		boolean isVerified = user.isVerified() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Verified.toString(), "" + isVerified ) ) ;
		
		boolean isContributorsEnabled = user.isContributorsEnabled() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Contributors_Enabled.toString(), "" + isContributorsEnabled ) ) ;
		
		boolean isFollowRequestSent = user.isFollowRequestSent() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Follow_Request_Sent.toString(), "" + isFollowRequestSent ) ) ;
		
		boolean isShowAllInlineMedia = user.isShowAllInlineMedia() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Show_All_Inline_Media.toString(), "" + isShowAllInlineMedia ) ) ;						
		
		boolean isTranslator = user.isTranslator() ;
		collector.emit( new Values( tupleType, subject.toString(), TWITTER.Is_Translator.toString(), "" + isTranslator ) ) ;													
		
		if( !isProtected )
		{
			//Get user-follower information
			Thread.sleep( TwitterConstants.TIME_DELAY_BETWEEN_REQUESTS ) ;
			long cursor = -1 ; IDs ids = twitter.getFollowersIDs( randomUserId, cursor ) ; 
			while( ids.getNextCursor() != 0 )
			{
				Thread.sleep( TwitterConstants.TIME_DELAY_BETWEEN_REQUESTS ) ;
				for( long id : ids.getIDs() )
				{
					StringBuilder follower = new StringBuilder( "res~" ) ; follower.append( TwitterConstants.TWITTER_USER_URI ) ; follower.append( id ) ;
					collector.emit( new Values( tupleType, subject.toString(), TWITTER.Has_Follower.toString(), follower.toString() ) ) ;
					
					StringBuilder subAsObj = new StringBuilder( "res~" ) ; subAsObj.append( subject.toString() ) ;
					collector.emit( new Values( tupleType, follower.toString(), TWITTER.Is_Follower_Of.toString(), subAsObj.toString() ) ) ;
					follower = null ; subAsObj = null ;
				}
				ids = twitter.getFollowersIDs( randomUserId, cursor ) ;
			}
			cursor = -1 ; ids = null ; ids = twitter.getFriendsIDs( randomUserId, cursor ) ;

			//Get user-friend information
			Thread.sleep( TwitterConstants.TIME_DELAY_BETWEEN_REQUESTS ) ;
			while( ids.getNextCursor() != 0 )
			{
				Thread.sleep( TwitterConstants.TIME_DELAY_BETWEEN_REQUESTS ) ;
				for( long id : ids.getIDs() )
				{
					StringBuilder friend = new StringBuilder( "res~" ) ; friend.append( TwitterConstants.TWITTER_USER_URI ) ; friend.append( id ) ;
					collector.emit( new Values( tupleType, subject.toString(), TWITTER.Has_Friend.toString(), friend.toString() ) ) ;

					StringBuilder subAsObj = new StringBuilder( "res~" ) ; subAsObj.append( subject.toString() ) ;
					collector.emit( new Values( tupleType, friend.toString(), TWITTER.Has_Friend.toString(), subject.toString() ) ) ;
					friend = null ; subAsObj = null ;
				}
				ids = twitter.getFriendsIDs( randomUserId, cursor ) ;
			}
			ids = null ; cursor = -1 ;
		}
		
		//Get user-tweet information
		Thread.sleep( TwitterConstants.TIME_DELAY_BETWEEN_REQUESTS ) ;
		int page = 1 ; Paging paging = new Paging( page, TwitterConstants.STATUSES_PER_PAGE ) ;
		List<Status> tweets = twitter.getUserTimeline( randomUserId, paging ) ; 
		while( !tweets.isEmpty() )
		{
			for( Status status : tweets )
			{
				StringBuilder tweet = new StringBuilder() ; tweet.append( TwitterConstants.TWITTER_TWEET_URI ) ; tweet.append( status.getId() ) ;
				StringBuilder tweetAsObj = new StringBuilder( "res~" ) ; tweetAsObj.append( tweet.toString() ) ;
				collector.emit( new Values( tupleType, subject.toString(), TWITTER.Has_Tweet.toString(), tweetAsObj.toString() ) ) ;
				
				Date statusCreatedAt = status.getCreatedAt() ;		
				collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_Created_At.toString(), statusCreatedAt.toString() ) ) ;
				
				StringBuilder statusText = new StringBuilder( "lit~" ) ; if( status.getText() != null ) statusText.append( status.getText() ) ; 							
				if( !statusText.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_Text.toString(), statusText.toString() ) ) ;
				statusText = null ;
				
				StringBuilder statusSource = new StringBuilder( "lit~" ) ; if( status.getSource() != null ) statusSource.append( status.getSource() ) ; 						
				if( !statusSource.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_Source.toString(), statusSource.toString() ) ) ;
				statusSource = null ;
				
				StringBuilder statusReplyStatusId = new StringBuilder( "res~" ) ; statusReplyStatusId.append( TwitterConstants.TWITTER_TWEET_URI ) ; statusReplyStatusId.append( status.getInReplyToStatusId() ) ; 		
				if( status.getInReplyToStatusId() > 0 ) collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_In_Reply_To_Status_Id.toString(), "" + statusReplyStatusId.toString() ) ) ;
				statusReplyStatusId = null ;
				
				StringBuilder statusReplyUserId = new StringBuilder( "res~" ) ; statusReplyUserId.append( TwitterConstants.TWITTER_USER_URI ) ; statusReplyUserId.append( status.getInReplyToUserId() ) ; 			
				if( status.getInReplyToUserId() > 0 ) collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_In_Reply_To_User_Id.toString(), "" + statusReplyUserId.toString() ) ) ;
				statusReplyUserId = null ;
				
				StringBuilder statusReplyScreenName = new StringBuilder( "res~" ) ; if( status.getInReplyToScreenName() != null ) { statusReplyScreenName.append( TwitterConstants.TWITTER_USER_URI ) ; statusReplyScreenName.append( status.getInReplyToScreenName() ) ; }	
				if( !statusReplyScreenName.toString().equals( "res~" ) ) collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_In_Reply_To_Screen_Name.toString(), statusReplyScreenName.toString() ) ) ;
				statusReplyScreenName = null ;
				
				StringBuilder statusRetweetCount = new StringBuilder( "lit~" ) ; if( status.getRetweetCount() > 0 ) statusRetweetCount.append( status.getRetweetCount() ) ; 						
				if( !statusRetweetCount.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_Retweet_Count.toString(), statusRetweetCount.toString() ) ) ;
				
				StringBuilder statusGeoLocation = new StringBuilder( "lit~" ) ; if( status.getGeoLocation() != null ) statusGeoLocation.append( status.getGeoLocation().toString() ) ; 	
				if( !statusGeoLocation.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_Geo.toString(), statusGeoLocation.toString() ) ) ;
				statusGeoLocation = null ;
				
				StringBuilder statusPlace = new StringBuilder( "lit~" ) ; if( status.getPlace() != null ) statusPlace.append( status.getPlace().toString() ) ;				
				if( !statusPlace.toString().equals( "lit~" ) ) collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_Place.toString(), statusPlace.toString() ) ) ;
				statusPlace = null ;
				
				long[] statusContributors = status.getContributors() ; 
				if( statusContributors != null && statusContributors.length > 0 ) 
				{
					for( long id : statusContributors ) 
					{
						StringBuilder contributor = new StringBuilder( "res~" ) ; contributor.append( TwitterConstants.TWITTER_USER_URI ) ; contributor.append( id ) ;
						collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Tweet_Contributors.toString(), contributor.toString() ) ) ;
						contributor = null ;
					}
				}
				boolean isTruncated = status.isTruncated() ;
				collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Is_Truncated.toString(), "" + isTruncated ) ) ;
				
				boolean isFavourited = status.isFavorited() ;
				collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Is_Favorited.toString(), "" + isFavourited ) ) ;
				
				boolean isRetweeted = status.isRetweet() ;
				collector.emit( new Values( tupleType, tweet.toString(), TWITTER.Is_Retweeted.toString(), "" + isRetweeted ) ) ;
			}
			Thread.sleep( TwitterConstants.TIME_DELAY_BETWEEN_REQUESTS ) ;
			page++ ; paging = null ; paging = new Paging( page, TwitterConstants.STATUSES_PER_PAGE ) ;
			tweets = twitter.getUserTimeline( randomUserId, paging ) ;
		}
    }
}