package liquibase.ext.db2i.sqlgenerator;

import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.statement.core.SetColumnRemarksStatement;
import liquibase.statement.core.SetTableRemarksStatement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RemarksGeneratorDB2iTest {

    private static final String CHARS_50 = "A123456789B123456789C123456789D123456789E123456789";
    private static final String CHARS_60 = CHARS_50 + "F123456789";

    private static String tableSql(String remarks) {
        Sql[] sql = new SetTableRemarksGeneratorDB2i().generateSql(
                new SetTableRemarksStatement(null, null, "T", remarks), new DB2iDatabase(), null);
        assertEquals(1, sql.length);
        return sql[0].toSql();
    }

    /** Returns {column text SQL, column heading SQL}. */
    private static String[] columnSql(String remarks) {
        Sql[] sql = new SetColumnRemarksGeneratorDB2i().generateSql(
                new SetColumnRemarksStatement(null, null, "T", "C", remarks), new DB2iDatabase(), null);
        assertEquals(2, sql.length);
        return new String[]{sql[0].toSql(), sql[1].toSql()};
    }

    @Test
    public void tableRemarksUpTo50AreUnchanged() {
        assertEquals("LABEL ON TABLE T IS '" + CHARS_50 + "'", tableSql(CHARS_50));
    }

    @Test
    public void tableRemarksAreTruncatedFromTheFirstCharacter() {
        assertEquals("LABEL ON TABLE T IS '" + CHARS_50 + "'", tableSql(CHARS_50 + "X"));
    }

    @Test
    public void columnTextIsTruncatedTo50AndHeadingTo60() {
        String[] sql = columnSql(CHARS_60 + "G");
        assertEquals("LABEL ON T (C TEXT IS '" + CHARS_50 + "')", sql[0]);
        assertEquals("LABEL ON COLUMN T.C IS '" + CHARS_60 + "'", sql[1]);
    }

    @Test
    public void nullRemarksBecomeEmpty() {
        assertEquals("LABEL ON TABLE T IS ''", tableSql(null));
        String[] sql = columnSql(null);
        assertEquals("LABEL ON T (C TEXT IS '')", sql[0]);
        assertEquals("LABEL ON COLUMN T.C IS ''", sql[1]);
    }

    @Test
    public void emptyRemarksStayEmpty() {
        assertEquals("LABEL ON TABLE T IS ''", tableSql(""));
    }

    @Test
    public void remarksAreTrimmedBeforeTruncation() {
        assertEquals("LABEL ON TABLE T IS '" + CHARS_50 + "'", tableSql("  " + CHARS_50 + "  "));
    }

    @Test
    public void quoteIsEscapedAfterTruncation() {
        // quote at position 50 is kept and doubled; the doubled quote does not count towards the limit
        String remarks = CHARS_50.substring(0, 49) + "'" + "XYZ";
        assertEquals("LABEL ON TABLE T IS '" + CHARS_50.substring(0, 49) + "'''", tableSql(remarks));
    }

    @Test
    public void umlautsCountAsOneCharacter() {
        String umlauts = repeat("Ä", 50);
        assertEquals("LABEL ON TABLE T IS '" + umlauts + "'", tableSql(umlauts + "Ö"));
    }

    @Test
    public void surrogatePairIsNotSplit() {
        // U+1F600 occupies positions 50 and 51 (one surrogate pair) and must be dropped completely
        String remarks = CHARS_50.substring(0, 49) + "😀" + "X";
        assertEquals("LABEL ON TABLE T IS '" + CHARS_50.substring(0, 49) + "'", tableSql(remarks));
    }

    private static String repeat(String s, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(s);
        }
        return sb.toString();
    }
}
