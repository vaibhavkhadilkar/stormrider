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

package edu.utdallas.cs.stormrider.vocab;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;

public class STORMRIDER 
{
	//URI to use for this ontology
    protected static final String uri = "http://cs.utdallas.edu/semanticweb/StormRider/vocabs/stormrider/0.1#";

    /**
     * Method to return the URI for this ontology
     * @return the URI
     */
    public static String getURI() { return uri; }

    //Create an in-memory Jena model used to create the properties
    private static Model m = ModelFactory.createDefaultModel();

    //Define the properties we need for the StormRider framework
    public static final Property Timestamp = m.createProperty( uri, "Timestamp" );
}