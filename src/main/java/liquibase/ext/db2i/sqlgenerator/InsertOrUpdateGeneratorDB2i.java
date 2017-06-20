package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.LiquibaseException;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InsertOrUpdateGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.InsertOrUpdateStatement;

import java.util.Date;

/**
 * @author Daniel Göbbels; daniel.goebbels@veda.net
 * @author Jan Ophey; jan.ophey@veda.net
 */
public class InsertOrUpdateGeneratorDB2i extends InsertOrUpdateGenerator {

	@Override
	public boolean supports(InsertOrUpdateStatement statement, Database database) {
		return database instanceof DB2iDatabase;
	}

	@Override
	public int getPriority() {
		return super.getPriority() + 5;
	}

	@Override
	protected String getRecordCheck(InsertOrUpdateStatement insertOrUpdateStatement, Database database,
	                                String whereClause) {
		StringBuilder sql = new StringBuilder();
		sql.append("MERGE INTO ")
				.append(buildTableName(insertOrUpdateStatement, database))
				.append(" AS DST ")
				.append("USING (")
				.append("VALUES (")
				.append(getValues(insertOrUpdateStatement, database))
				.append(") ) AS SRC( ")
				.append(buildColumns(insertOrUpdateStatement))
				.append(") ON ");
		String[] keys = insertOrUpdateStatement.getPrimaryKey().split(",");
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
	protected String getInsertStatement(InsertOrUpdateStatement insertOrUpdateStatement, Database database,
	                                    SqlGeneratorChain sqlGeneratorChain) {
		StringBuilder columns = new StringBuilder();
		StringBuilder values = new StringBuilder();
		for (String columnKey : insertOrUpdateStatement.getColumnValues().keySet()) {
			columns.append(", ");
			columns.append(columnKey);
			values.append(", ");
			if (insertOrUpdateStatement.getColumnValues().get(columnKey).toString().equalsIgnoreCase("NULL")) {
				values.append("NULL");
			} else {
				values.append("SRC.").append(columnKey);
			}
		}
		columns.deleteCharAt(0);
		values.deleteCharAt(0);
		return " INSERT(" + columns.toString() + ")\n\t\tVALUES(" + values.toString() + ") ";
	}

	@Override
	protected String getElse(Database database) {
		return " WHEN MATCHED THEN ";
	}

	@Override
	protected String getUpdateStatement(InsertOrUpdateStatement statement, Database database, String whereClause, SqlGeneratorChain sqlGeneratorChain) throws LiquibaseException {
		StringBuilder sql = new StringBuilder("UPDATE SET ");
		for (String column : statement.getColumnValues().keySet()) {
			sql.append(" ")
					.append(column)
					.append(" = ")
					.append(getValueAsDatabaseType(statement.getColumnValues().get(column), database, false))
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

	private String buildTableName(InsertOrUpdateStatement insertOrUpdateStatement, Database database) {
		return database.escapeTableName(
				insertOrUpdateStatement.getCatalogName(),
				insertOrUpdateStatement.getSchemaName(),
				insertOrUpdateStatement.getTableName()
		);
	}

	private StringBuilder buildColumns(InsertOrUpdateStatement insertOrUpdateStatement) {
		StringBuilder columns = new StringBuilder();
		for (String columnKey : insertOrUpdateStatement.getColumnValues().keySet()) {

			if (insertOrUpdateStatement.getColumnValues().get(columnKey).toString().equalsIgnoreCase("NULL")) {
				continue;
			}
			columns.append(",");
			columns.append(columnKey);
		}
		columns.deleteCharAt(0);
		return columns;
	}


	private String getValueAsDatabaseType(Object newValue, Database database, boolean skipNullValue) {
		if (newValue == null || newValue.toString().equalsIgnoreCase("NULL")) {
			if (skipNullValue) {
				return "";
			} else {
				return "NULL";
			}
		} else if (newValue instanceof String && !looksLikeFunctionCall(((String) newValue), database)) {
			return DataTypeFactory.getInstance().fromObject(newValue, database).objectToSql(newValue, database);
		} else if (newValue instanceof Date) {
			return database.getDateLiteral(((Date) newValue));
		} else if (newValue instanceof Boolean) {
			if (((Boolean) newValue)) {
				return DataTypeFactory.getInstance().getTrueBooleanValue(database);
			} else {
				return DataTypeFactory.getInstance().getFalseBooleanValue(database);
			}
		} else if (newValue instanceof DatabaseFunction) {
			return database.generateDatabaseFunctionValue((DatabaseFunction) newValue);
		} else {
			return newValue.toString();
		}
	}

	private String getValues(InsertOrUpdateStatement statement, Database database) {
		StringBuilder values = new StringBuilder();
		for (String column : statement.getColumnValues().keySet()) {
			final String newValue = getValueAsDatabaseType(statement.getColumnValues().get(column), database, true);
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
