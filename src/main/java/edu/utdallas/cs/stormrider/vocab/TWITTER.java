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

package edu.utdallas.cs.stormrider.vocab;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;

/**
 * A TBox for the Twitter dataset with properties that cannot be mapped to existing ontologies
 * @author vaibhav
 *
 */
public class TWITTER 
{
	//URI to use for this ontology
    protected static final String uri = "http://cs.utdallas.edu/semanticweb/StormRider/vocabs/twitter/0.1#";

    /**
     * Method to return the URI for this ontology
     * @return the URI
     */
    public static String getURI() { return uri; }

    //Create an in-memory Jena model used to create the properties
    private static Model m = ModelFactory.createDefaultModel();

    //Define the properties we need for the Twitter user profile
    public static final Property Location = m.createProperty( uri, "Location" );
    public static final Property Description = m.createProperty( uri, "Description" );
    public static final Property Followers_Count = m.createProperty( uri, "Followers_Count" );
    public static final Property Friends_Count = m.createProperty( uri, "Friends_Count" );
    public static final Property Screen_Name = m.createProperty( uri, "Screen_Name" );
    public static final Property Created_At = m.createProperty( uri, "Created_At" );
    public static final Property Favourites_Count = m.createProperty( uri, "Favorites_Count" );
    public static final Property Statuses_Count = m.createProperty( uri, "Statuses_Count" );
    public static final Property Has_Friend = m.createProperty( uri, "Has_Friend" );
    public static final Property Has_Follower = m.createProperty( uri, "Has_Follower" );
    public static final Property Is_Follower_Of = m.createProperty( uri, "Is_Follower_Of" );
    public static final Property UTC_Offset = m.createProperty( uri, "UTC_Offset" );
    public static final Property Lang = m.createProperty( uri, "Lang" );
    public static final Property Listed_Count = m.createProperty( uri, "Listed_Count" );
    
    //Define the properties needed for a Twitter user's style information
    public static final Property Profile_Background_Color = m.createProperty( uri, "Profile_Background_Color" );
    public static final Property Profile_Text_Color = m.createProperty( uri, "Profile_Text_Color" );
    public static final Property Profile_Link_Color = m.createProperty( uri, "Profile_Link_Color" );
    public static final Property Profile_Sidebar_Fill_Color = m.createProperty( uri, "Profile_Sidebar_Fill_Color" );
    public static final Property Profile_Sidebar_Border_Color = m.createProperty( uri, "Profile_Sidebar_Border_Color" );
    public static final Property Profile_Background_Image_Url = m.createProperty( uri, "Profile_Background_Image_Url" );
    
    //Define the properties needed for a user's latest tweet
    public static final Property Has_Tweet = m.createProperty( uri, "Has_Tweet" );
    public static final Property Tweet_Created_At = m.createProperty( uri, "Tweet_Created_At" );
    public static final Property Tweet_Text = m.createProperty( uri, "Tweet_Text" );
    public static final Property Tweet_Source = m.createProperty( uri, "Tweet_Source" );
    public static final Property Tweet_In_Reply_To_Status_Id = m.createProperty( uri, "Tweet_In_Reply_To_Status_Id" );
    public static final Property Tweet_In_Reply_To_User_Id = m.createProperty( uri, "Tweet_In_Reply_To_User_Id" );
    public static final Property Tweet_In_Reply_To_Screen_Name = m.createProperty( uri, "Tweet_In_Reply_To_Screen_Name" );
    public static final Property Tweet_Retweet_Count = m.createProperty( uri, "Retweet_Count" ) ;
    public static final Property Tweet_Geo = m.createProperty( uri, "Tweet_Geo" ) ;
    public static final Property Tweet_Coordinates = m.createProperty( uri, "Tweet_Coordinates" ) ;
    public static final Property Tweet_Place = m.createProperty( uri, "Tweet_Place" ) ;
    public static final Property Tweet_Contributors = m.createProperty( uri, "Tweet_Contributors" ) ;
    
    //Define the boolean variables used in a user's profile
    public static final Property Is_Profile_Background_Tiled = m.createProperty( uri, "Is_Profile_Background_Tiled" );
    public static final Property Is_Profile_Background_Image_Used = m.createProperty( uri, "Is_Profile_Background_Image_Used" );
    public static final Property Is_Protected = m.createProperty( uri, "Is_Protected" );
    public static final Property Has_Notifications = m.createProperty( uri, "Has_Notifications" );
    public static final Property Is_Geo_Enabled = m.createProperty( uri, "Is_Geo_Enabled" );
    public static final Property Is_Verified = m.createProperty( uri, "Is_Verified" );
    public static final Property Is_Following = m.createProperty( uri, "Is_Following" );
    public static final Property Is_Truncated = m.createProperty( uri, "Is_Truncated" );
    public static final Property Is_Favorited = m.createProperty( uri, "Is_Favorited" );
    public static final Property Is_Retweeted = m.createProperty( uri, "Is_Retweeted" ) ;
    public static final Property Is_Contributors_Enabled = m.createProperty( uri, "Is_Contributors_Enabled" ) ;
    public static final Property Is_Follow_Request_Sent = m.createProperty( uri, "Is_Follow_Request_Sent" ) ;
    public static final Property Is_Show_All_Inline_Media = m.createProperty( uri, "Is_Show_All_Inline_Media" ) ;
    public static final Property Is_Translator = m.createProperty( uri, "Is_Translator" ) ;
}