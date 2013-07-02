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

package edu.utdallas.cs.stormrider.views.impl;

import edu.utdallas.cs.stormrider.views.ViewsBase;
import edu.utdallas.cs.stormrider.views.materialized.MaterializedViewsFactory;

public class ViewsImpl extends ViewsBase
{
	private static final long serialVersionUID = -2557107070742120071L;

	public ViewsImpl( String configFile ) { super( MaterializedViewsFactory.create( configFile ) ) ; }
}
