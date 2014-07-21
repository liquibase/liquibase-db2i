package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SetNullableGenerator;
import liquibase.statement.core.SetNullableStatement;

public class SetNullableGeneratorDB2i extends SetNullableGenerator {
	@Override
	public boolean supports(SetNullableStatement statement, Database database) {
		return database instanceof DB2iDatabase;
	}

	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public ValidationErrors validate(SetNullableStatement setNullableStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
		ValidationErrors validationErrors = new ValidationErrors();

		validationErrors.checkRequiredField("tableName", setNullableStatement.getTableName());
		validationErrors.checkRequiredField("columnName", setNullableStatement.getColumnName());

		try {
			if ((database instanceof DB2iDatabase) && (database.getDatabaseMajorVersion() > 0 && database.getDatabaseMajorVersion() < 4)) {
				validationErrors.addError("DB2i versions less than 4 do not support modifying null constraints");
			}
		} catch (DatabaseException ignore) {
			// cannot check
		}
		return validationErrors;

	}

}
