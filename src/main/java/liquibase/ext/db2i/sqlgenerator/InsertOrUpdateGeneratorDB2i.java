package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InsertOrUpdateGenerator;
import liquibase.sqlgenerator.core.InsertOrUpdateGeneratorDB2;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.InsertOrUpdateStatement;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DBMS-specific {@link InsertOrUpdateGenerator} for IBM DB2 running on IBM iSeries. Uses "MERGE"-Statement with a
 * 10x - 15x performance-boost compared to {@link liquibase.sqlgenerator.core.InsertOrUpdateGeneratorDB2} which is the
 * default implementation that is also compatible with DB2/LUW (Linux, Unix, Windows)
 * <br>
 * Needs IBM iSeries OS release V7R1M0 or newer.
 * This is the first publicly available V7 release. V6 and older are EOL.
 *
 * @author Daniel Göbbels; daniel.goebbels@veda.net
 * @author Jan Ophey; jan.ophey@veda.net
 * @see <a href="https://www-01.ibm.com/support/knowledgecenter/ssw_ibm_i_71/sqlp/rbafymerge.htm">
 * IBM Knowledge Center: Database > Programming > SQL programming > Data manipulation language > Merging data</a>
 */
public class InsertOrUpdateGeneratorDB2i extends InsertOrUpdateGenerator {

	@Override
	public boolean supports(InsertOrUpdateStatement stmt, Database db) {
		return db instanceof DB2iDatabase;
	}

	@Override
	public int getPriority() {
		//prefer us over default db2 implementation if applicable
		return new InsertOrUpdateGeneratorDB2().getPriority() + 1;
	}

	/**
	 * Builds the complete MERGE statement instead of relying on the template methods of the base class, because the
	 * base class can neither omit the update part without logging a warning per row nor express {@code onlyUpdate}
	 * for a MERGE.
	 * <ul>
	 * <li>Only columns that are not part of the primary key and for which
	 * {@link InsertOrUpdateStatement#getAllowColumnUpdate(String)} is true (i.e. {@code allowUpdate} is not
	 * {@code false}) are updated.</li>
	 * <li>If there is no such column, {@code WHEN MATCHED} is omitted (insert if missing).</li>
	 * <li>If {@code onlyUpdate} is set, {@code WHEN NOT MATCHED} is omitted; if there is nothing to update either,
	 * no SQL is generated.</li>
	 * </ul>
	 */
	@Override
	public Sql[] generateSql(InsertOrUpdateStatement stmt, Database db, SqlGeneratorChain sqlGeneratorChain) {
		boolean onlyUpdate = Boolean.TRUE.equals(stmt.getOnlyUpdate());
		List<String> updatableColumns = getUpdatableColumns(stmt);
		if (onlyUpdate && updatableColumns.isEmpty()) {
			return new Sql[0];
		}

		StringBuilder sql = new StringBuilder(buildMergeHead(stmt, db));
		if (!onlyUpdate) {
			sql.append(" WHEN NOT MATCHED THEN ")
					.append(getInsertStatement(stmt, db, sqlGeneratorChain));
		}
		if (!updatableColumns.isEmpty()) {
			sql.append(getElse(db))
					.append(buildUpdateSet(stmt, db, updatableColumns));
		}
		return new Sql[]{new UnparsedSql(sql.toString(), "", getAffectedTable(stmt))};
	}

	@Override
	protected String getRecordCheck(InsertOrUpdateStatement stmt, Database db, String whereClause) {
		// not used by generateSql, implemented because it is abstract in the base class
		return buildMergeHead(stmt, db) + " WHEN NOT MATCHED THEN ";
	}

	private String buildMergeHead(InsertOrUpdateStatement stmt, Database db) {
		StringBuilder sql = new StringBuilder("MERGE INTO ")
				.append(db.escapeTableName(stmt.getCatalogName(), stmt.getSchemaName(), stmt.getTableName()))
				.append(" AS DST ")
				.append("USING (")
				.append("VALUES (")
				.append(getValues(stmt, db))
				.append(") ) AS SRC( ")
				.append(buildColumns(stmt))
				.append(") ON ");
		for (String key : getPrimaryKeyColumns(stmt)) {
			sql.append("DST.").append(key)
					.append(" = SRC.").append(key)
					.append(" AND ");
		}
		sql.delete(sql.length() - " AND ".length(), sql.length()); //remove trailing ' AND '
		return sql.toString();
	}

	private List<String> getPrimaryKeyColumns(InsertOrUpdateStatement stmt) {
		List<String> keys = new ArrayList<>();
		for (String key : stmt.getPrimaryKey().split(",")) {
			String trimmed = key.trim();
			if (!trimmed.isEmpty()) {
				keys.add(trimmed);
			}
		}
		return keys;
	}

	private List<String> getUpdatableColumns(InsertOrUpdateStatement stmt) {
		List<String> primaryKeyColumns = getPrimaryKeyColumns(stmt);
		List<String> updatable = new ArrayList<>();
		for (String column : stmt.getColumnValues().keySet()) {
			boolean isPrimaryKey = false;
			for (String key : primaryKeyColumns) {
				if (key.equalsIgnoreCase(column.trim())) {
					isPrimaryKey = true;
					break;
				}
			}
			if (!isPrimaryKey && stmt.getAllowColumnUpdate(column)) {
				updatable.add(column);
			}
		}
		return updatable;
	}

	@Override
	protected String getInsertStatement(InsertOrUpdateStatement stmt, Database db, SqlGeneratorChain sqlGeneratorChain) {
		StringBuilder columns = new StringBuilder();
		StringBuilder values = new StringBuilder();
		for (String columnKey : stmt.getColumnValues().keySet()) {
			columns.append(", ");
			columns.append(columnKey);
			values.append(", ");
			if (stmt.getColumnValues().get(columnKey).toString().equalsIgnoreCase("NULL")) {
				values.append("NULL");
			} else {
				values.append("SRC.").append(columnKey);
			}
		}
		columns.deleteCharAt(0); // remove leading commas
		values.deleteCharAt(0);
		return " INSERT(" + columns.toString() + ") VALUES (" + values.toString() + ") ";
	}

	@Override
	protected String getElse(Database db) {
		return " WHEN MATCHED THEN ";
	}

	private String buildUpdateSet(InsertOrUpdateStatement stmt, Database db, List<String> updatableColumns) {
		StringBuilder sql = new StringBuilder("UPDATE SET ");
		for (String column : updatableColumns) {
			sql.append(" ")
					.append(column)
					.append(" = ")
					.append(getValueAsDatabaseType(stmt.getColumnValues().get(column), db, false))
					.append(", ");
		}
		sql.deleteCharAt(sql.lastIndexOf(" "));
		int lastComma = sql.lastIndexOf(",");
		if (lastComma >= 0) {
			sql.deleteCharAt(lastComma);
		}
		sql.append(" ");
		return sql.toString();
	}

	private StringBuilder buildColumns(InsertOrUpdateStatement stmt) {
		StringBuilder columns = new StringBuilder();
		for (String columnKey : stmt.getColumnValues().keySet()) {
			if (stmt.getColumnValues().get(columnKey).toString().equalsIgnoreCase("NULL")) {
				continue;
			}
			columns.append(",");
			columns.append(columnKey);
		}
		columns.deleteCharAt(0);
		return columns;
	}


	private String getValueAsDatabaseType(Object newValue, Database db, boolean skipNullValue) {
		if (newValue == null || newValue.toString().equalsIgnoreCase("NULL")) {
			return skipNullValue ? "" : "NULL";
		} else if (newValue instanceof String && !looksLikeFunctionCall(((String) newValue), db)) {
			return DataTypeFactory.getInstance().fromObject(newValue, db).objectToSql(newValue, db);
		} else if (newValue instanceof Date) {
			return db.getDateLiteral(((Date) newValue));
		} else if (newValue instanceof Boolean) {
			if (((Boolean) newValue)) {
				return DataTypeFactory.getInstance().getTrueBooleanValue(db);
			} else {
				return DataTypeFactory.getInstance().getFalseBooleanValue(db);
			}
		} else if (newValue instanceof DatabaseFunction) {
			return db.generateDatabaseFunctionValue((DatabaseFunction) newValue);
		} else {
			return newValue.toString();
		}
	}

	private String getValues(InsertOrUpdateStatement stmt, Database db) {
		StringBuilder values = new StringBuilder();
		for (String column : stmt.getColumnValues().keySet()) {
			final String newValue = getValueAsDatabaseType(stmt.getColumnValues().get(column), db, true);
			if (newValue != null && newValue.length() > 0) {
				values.append(newValue)
						.append(", ");
			}
		}
		values.deleteCharAt(values.lastIndexOf(" "));
		int lastComma = values.lastIndexOf(",");
		if (lastComma >= 0) {
			values.deleteCharAt(lastComma);
		}
		return values.toString();
	}

}
