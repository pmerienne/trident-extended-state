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

import static com.github.pmerienne.trident.state.cassandra.dao.CassandraDao.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import storm.trident.state.Serializer;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.github.pmerienne.trident.state.SparseMatrixState.SparseVector;
import com.google.common.collect.ImmutableMap;

public class SparseMatrixQueryTemplate extends AbstractQueryTemplate {

	private final Session session;
	private final Map<Operation, PreparedStatement> preparedStatements;

	public SparseMatrixQueryTemplate(Session session) {
		this.session = session;
		this.preparedStatements = prepareSparseMatrixStatements();
	}

	private Map<Operation, PreparedStatement> prepareSparseMatrixStatements() {
		PreparedStatement sparseMatrixInsertRow = session.prepare(new SimpleStatement("INSERT INTO "
				+ SPARSE_MATRIX_ROW_STATE_TABLE + "(id,row_index,column_index,value) VALUES(?,?,?,?)"));
		PreparedStatement sparseMatrixInsertColumn = session.prepare(new SimpleStatement("INSERT INTO "
				+ SPARSE_MATRIX_COLUMN_STATE_TABLE + "(id,row_index,column_index,value) VALUES(?,?,?,?)"));

		PreparedStatement sparseMatrixGet = session.prepare(new SimpleStatement("SELECT value FROM "
				+ SPARSE_MATRIX_ROW_STATE_TABLE + " WHERE id=? AND row_index=? AND column_index=?"));

		PreparedStatement sparseMatrixGetRow = session.prepare(new SimpleStatement("SELECT column_index,value FROM "
				+ SPARSE_MATRIX_ROW_STATE_TABLE + " WHERE id=? AND row_index=?"));
		PreparedStatement sparseMatrixGetColumn = session.prepare(new SimpleStatement("SELECT row_index,value FROM "
				+ SPARSE_MATRIX_COLUMN_STATE_TABLE + " WHERE id=? AND column_index=?"));

		return ImmutableMap.of(Operation.INSERT_ROW, sparseMatrixInsertRow, Operation.INSERT_COLUMN,
				sparseMatrixInsertColumn, Operation.GET, sparseMatrixGet, Operation.GET_ROW, sparseMatrixGetRow,
				Operation.GET_COLUMN, sparseMatrixGetColumn);
	}

	public <T> T get(String id, long i, long j, Serializer<T> serializer) {
		T result = null;

		PreparedStatement ps = preparedStatements.get(Operation.GET);
		Row row = session.execute(ps.bind(id, i, j)).one();
		if (row != null) {
			result = deserialize(row.getBytes("value"), serializer);
		}
		return result;
	}

	public <T> void set(String id, long i, long j, T value, Serializer<T> serializer) {
		PreparedStatement psRow = preparedStatements.get(Operation.INSERT_ROW);
		PreparedStatement psColumn = preparedStatements.get(Operation.INSERT_COLUMN);
		BatchStatement batch = new BatchStatement();

		ByteBuffer serialized = serialize(value, serializer);

		batch.add(psRow.bind(id, i, j, serialized));
		batch.add(psColumn.bind(id, i, j, serialized));

		session.execute(batch);
	}

	public <T> SparseVector<T> getColumnsFromRow(String id, long rowIndex, Serializer<T> serializer) {
		SparseVector<T> result = new SparseVector<T>();

		PreparedStatement ps = preparedStatements.get(Operation.GET_ROW);
		List<Row> rows = session.execute(ps.bind(id, rowIndex)).all();
		for (Row row : rows) {
			Long columnIndex = row.getLong("column_index");
			T value = deserialize(row.getBytes("value"), serializer);
			result.put(columnIndex, value);
		}
		return result;
	}

	public <T> SparseVector<T> getRowsFromColumn(String id, long columnIndex, Serializer<T> serializer) {
		SparseVector<T> result = new SparseVector<T>();

		PreparedStatement ps = preparedStatements.get(Operation.GET_COLUMN);
		List<Row> rows = session.execute(ps.bind(id, columnIndex)).all();
		for (Row row : rows) {
			Long rowIndex = row.getLong("row_index");
			T value = deserialize(row.getBytes("value"), serializer);
			result.put(rowIndex, value);
		}
		return result;
	}
}
