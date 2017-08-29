package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.LiquibaseException;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InsertOrUpdateGenerator;
import liquibase.sqlgenerator.core.InsertOrUpdateGeneratorDB2;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.InsertOrUpdateStatement;

import java.util.Date;

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

	@Override
	protected String getRecordCheck(InsertOrUpdateStatement stmt, Database db, String whereClause) {
		StringBuilder sql = new StringBuilder("MERGE INTO ")
				.append(db.escapeTableName(stmt.getCatalogName(), stmt.getSchemaName(), stmt.getTableName()))
				.append(" AS DST ")
				.append("USING (")
				.append("VALUES (")
				.append(getValues(stmt, db))
				.append(") ) AS SRC( ")
				.append(buildColumns(stmt))
				.append(") ON ");
		String[] keys = stmt.getPrimaryKey().split(",");
		for (String key : keys) {
			sql.append("DST.").append(key)
					.append(" = SRC.").append(key)
					.append(" AND ");
		}
		sql.delete(sql.length() - " AND ".length(), sql.length()); //remove trailing ' AND '
		sql.append(" WHEN NOT MATCHED THEN ");
		return sql.toString();
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

	@Override
	protected String getUpdateStatement(InsertOrUpdateStatement stmt, Database db, String whereClause, SqlGeneratorChain sqlGeneratorChain) throws LiquibaseException {
		StringBuilder sql = new StringBuilder("UPDATE SET ");
		for (String column : stmt.getColumnValues().keySet()) {
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
