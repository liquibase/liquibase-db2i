package liquibase.ext.db2i.sqlgenerator;

import liquibase.exception.ValidationErrors;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.PrimaryKeyConstraint;
import liquibase.statement.core.AddColumnStatement;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Runs through {@link SqlGeneratorFactory}, so these tests also check that {@link AddColumnGeneratorDB2i} is
 * registered and wins over the liquibase-core generators.
 */
public class AddColumnGeneratorDB2iTest {

    private final DB2iDatabase database = new DB2iDatabase();

    private static AddColumnStatement column(String name) {
        return new AddColumnStatement(null, null, "T", name, "INT", null);
    }

    private ValidationErrors validate(AddColumnStatement statement) {
        return SqlGeneratorFactory.getInstance().validate(statement, database);
    }

    private String sql(AddColumnStatement statement) {
        Sql[] sql = SqlGeneratorFactory.getInstance().generateSql(statement, database);
        assertEquals(1, sql.length);
        return sql[0].toSql();
    }

    @Test
    public void db2iGeneratorIsUsed() {
        assertTrue(SqlGeneratorFactory.getInstance().getGenerators(column("C1"), database).first()
                instanceof AddColumnGeneratorDB2i);
    }

    @Test
    public void beforeColumnIsAllowedAndGenerated() {
        AddColumnStatement statement = column("C1");
        statement.setAddBeforeColumn("B");
        assertFalse(validate(statement).toString(), validate(statement).hasErrors());
        assertTrue(sql(statement), sql(statement).endsWith(" BEFORE B"));
    }

    @Test
    public void afterColumnIsRejected() {
        AddColumnStatement statement = column("C1");
        statement.setAddAfterColumn("A");
        assertEquals("addAfterColumn is not allowed on db2i", validate(statement).toString());
    }

    @Test
    public void positionIsRejected() {
        AddColumnStatement statement = column("C1");
        statement.setAddAtPosition(2);
        assertEquals("addAtPosition is not allowed on db2i", validate(statement).toString());
    }

    @Test
    public void beforeColumnIsAllowedForMultipleColumns() {
        AddColumnStatement c1 = column("C1");
        c1.setAddBeforeColumn("B");
        AddColumnStatement c2 = column("C2");
        c2.setAddAfterColumn("A");
        assertEquals("addAfterColumn is not allowed on db2i", validate(new AddColumnStatement(c1, c2)).toString());
    }

    @Test
    public void primaryKeyColumnIsRejected() {
        AddColumnStatement statement = new AddColumnStatement(null, null, "T", "C1", "INT", null,
                new PrimaryKeyConstraint());
        assertEquals("Cannot add a primary key column", validate(statement).toString());
    }

    @Test
    public void notNullWithConstraintNameHasNoConstraintClause() {
        AddColumnStatement statement = new AddColumnStatement(null, null, "T", "C1", "INT", 5,
                new NotNullConstraint().setConstraintName("NN1"));
        assertEquals("ALTER TABLE T ADD C1 INTEGER DEFAULT 5 NOT NULL", sql(statement));
    }

    @Test
    public void notNullWithoutDefaultIsRejected() {
        AddColumnStatement statement = new AddColumnStatement(null, null, "T", "C1", "INT", null,
                new NotNullConstraint());
        assertEquals(
                "Adding NOT NULL column 'C1' requires a default value on db2i (defaultValue or defaultValueComputed)",
                validate(statement).toString());
    }

    @Test
    public void notNullWithoutDefaultIsRejectedForMultipleColumns() {
        AddColumnStatement c1 = column("C1");
        AddColumnStatement c2 = new AddColumnStatement(null, null, "T", "C2", "INT", null,
                new NotNullConstraint());
        assertEquals(
                "Adding NOT NULL column 'C2' requires a default value on db2i (defaultValue or defaultValueComputed)",
                validate(new AddColumnStatement(c1, c2)).toString());
    }

    @Test
    public void notNullWithDefaultIsAllowed() {
        AddColumnStatement statement = new AddColumnStatement(null, null, "T", "C1", "INT", 0,
                new NotNullConstraint());
        assertFalse(validate(statement).toString(), validate(statement).hasErrors());
    }

    @Test
    public void notNullIdentityWithoutDefaultIsAllowed() {
        AddColumnStatement statement = new AddColumnStatement(null, null, "T", "C1", "INT", null,
                new NotNullConstraint(), new AutoIncrementConstraint("C1"));
        assertFalse(validate(statement).toString(), validate(statement).hasErrors());
    }

    @Test
    public void nullableWithoutDefaultIsAllowed() {
        assertFalse(validate(column("C1")).hasErrors());
    }

    @Test
    public void autoIncrementColumnGetsIdentityClause() {
        AddColumnStatement statement = new AddColumnStatement(null, null, "T", "C1", "INT", null,
                new AutoIncrementConstraint("C1", BigInteger.valueOf(100), BigInteger.valueOf(5), null, null));
        assertTrue(sql(statement), sql(statement).endsWith(
                " GENERATED BY DEFAULT AS IDENTITY (START WITH 100, INCREMENT BY 5)"));
    }
}
