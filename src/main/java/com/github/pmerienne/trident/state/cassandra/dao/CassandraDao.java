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
package com.github.pmerienne.trident.state.cassandra.dao;

import com.datastax.driver.core.Session;

public class CassandraDao {

	public static final String SET_STATE_TABLE = "set_state";
	public static final String MAP_STATE_TABLE = "map_state";
	public static final String SET_MULTI_MAP_STATE_TABLE = "set_multi_map_state";
	public static final String MAP_MULTI_MAP_STATE_TABLE = "map_multi_map_state";
	public static final String SPARSE_MATRIX_ROW_STATE_TABLE = "sparse_matrix_row_state";
	public static final String SPARSE_MATRIX_COLUMN_STATE_TABLE = "sparse_matrix_column_state";

	private final SetQueryTemplate setTemplate;
	private final MapQueryTemplate mapTemplate;
	private final SetMultiMapQueryTemplate setMultiMapTemplate;
	private final MapMultiMapQueryTemplate mapMultiMapTemplate;
	private final SparseMatrixQueryTemplate sparseMatrixTemplate;

	public <T> CassandraDao(Session session) {
		createTableIfNotExists(session);

		this.setTemplate = new SetQueryTemplate(session);
		this.mapTemplate = new MapQueryTemplate(session);
		this.setMultiMapTemplate = new SetMultiMapQueryTemplate(session);
		this.mapMultiMapTemplate = new MapMultiMapQueryTemplate(session);
		this.sparseMatrixTemplate = new SparseMatrixQueryTemplate(session);
	}

	public static void createTableIfNotExists(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS " + SET_STATE_TABLE + "(id text,value blob, PRIMARY KEY(id,value))");
		session.execute("CREATE TABLE IF NOT EXISTS " + MAP_STATE_TABLE
				+ "(id text,map_key text,value blob,PRIMARY KEY(id,map_key))");

		session.execute("CREATE TABLE IF NOT EXISTS " + SET_MULTI_MAP_STATE_TABLE
				+ "(id text,key blob,column blob,value blob,PRIMARY KEY((id,key),column))");
		session.execute("CREATE TABLE IF NOT EXISTS " + MAP_MULTI_MAP_STATE_TABLE
				+ "(id text,outer_key blob,inner_key blob,value blob,PRIMARY KEY((id,outer_key),inner_key))");

		session.execute("CREATE TABLE IF NOT EXISTS " + SPARSE_MATRIX_ROW_STATE_TABLE
				+ "(id text,row_index bigint,column_index bigint,value blob,PRIMARY KEY((id,row_index),column_index))");
		session.execute("CREATE TABLE IF NOT EXISTS " + SPARSE_MATRIX_COLUMN_STATE_TABLE
				+ "(id text,column_index bigint,row_index bigint,value blob,PRIMARY KEY((id,column_index),row_index))");
	}

	public SetQueryTemplate getSetQueryTemplate() {
		return setTemplate;
	}

	public MapQueryTemplate getMapQueryTemplate() {
		return mapTemplate;
	}

	public SetMultiMapQueryTemplate getSetMultiMapQueryTemplate() {
		return setMultiMapTemplate;
	}

	public MapMultiMapQueryTemplate getMapMultiMapQueryTemplate() {
		return mapMultiMapTemplate;
	}

	public SparseMatrixQueryTemplate getSparseMatrixQueryTemplate() {
		return sparseMatrixTemplate;
	}

}
