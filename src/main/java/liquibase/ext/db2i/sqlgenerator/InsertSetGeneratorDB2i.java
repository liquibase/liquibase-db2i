/*
 * Copyright (c) 2017. VEDA GmbH. All rights reserved.
 * Use is subject to license terms.
 */
package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InsertSetGenerator;
import liquibase.statement.core.InsertSetStatement;
import liquibase.statement.core.InsertStatement;

import java.util.ArrayList;

/**
 * DBMS-specific {@link InsertSetGenerator} for IBM DB2 running on IBM iSeries.
 *
 * Removes forced semicolon at end of statement.
 *
 * @author Roland Doepke; roland.doepke@veda.net
 */
public class InsertSetGeneratorDB2i extends InsertSetGenerator {

    @Override
    public boolean supports(InsertSetStatement stmt, Database db) {
        return db instanceof DB2iDatabase;
    }

    @Override
    public int getPriority() {
        //prefer us over default db2 implementation if applicable
        return new InsertSetGenerator().getPriority() + 1;
    }

    @Override
    public Sql[] generateSql(InsertSetStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        if (statement.peek() == null) {
            return new UnparsedSql[0];
        }
        StringBuilder sql = new StringBuilder();
        generateHeader(sql, statement, database);

        ArrayList<Sql> result = new ArrayList<Sql>();
        int index = 0;
        for (InsertStatement sttmnt : statement.getStatements()) {
            index++;
            getInsertGenerator(database).generateValues(sql, sttmnt, database);
            sql.append(",");
            if (index > statement.getBatchThreshold()) {
                result.add(completeStatement(statement, sql));

                index = 0;
                sql = new StringBuilder();
                generateHeader(sql, statement, database);
            }
        }
        if (index > 0) {
            result.add(completeStatement(statement, sql));
        }

        return result.toArray(new UnparsedSql[result.size()]);
    }

    private Sql completeStatement(InsertSetStatement statement, StringBuilder sql) {
        sql.deleteCharAt(sql.lastIndexOf(","));
        return new UnparsedSql(sql.toString(), getAffectedTable(statement));
    }
}
