/**
 * Copyright 2013-2015 Pierre Merienne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.pmerienne.trident.state.cassandra.embedded;

import com.github.pmerienne.trident.state.cassandra.embedded.AbstractTestResource.Steps;

public class CQLResourceBuilder {

	/**
	 * Truncate tables BEFORE each test
	 */
	public static CQLResource truncateBeforeTest() {
		return new CQLResource(Steps.BEFORE_TEST);
	}

	/**
	 * Truncate tables AFTER each test
	 */
	public static CQLResource truncateAfterTest() {
		return new CQLResource(Steps.AFTER_TEST);
	}

	/**
	 * Truncate tables BEFORE and AFTER each test
	 */
	public static CQLResource truncateBeforeAndAfterTest() {
		return new CQLResource(Steps.BOTH);
	}


    /**
     * DO NOT truncate tables to speed up perf
     */
    public static CQLResource noTruncate() {
        return new CQLResource();
    }
}
