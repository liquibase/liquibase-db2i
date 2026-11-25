package liquibase.ext.db2i.sqlgenerator;

import liquibase.Scope;
import liquibase.util.StringUtil;

/**
 * Length limits of {@code LABEL ON} on DB2 for IBM i. Longer texts are rejected with SQL0107, so remarks are
 * truncated before they are written.
 * <p>
 * The limits are in bytes. With a single-byte job CCSID (e.g. 273) one character is one byte, umlauts included,
 * so counting characters is sufficient there.
 */
final class RemarksUtilDB2i {

    /** Object text ({@code LABEL ON TABLE ... IS}) and column text ({@code LABEL ON ... (col TEXT IS ...)}). */
    static final int MAX_TEXT_LENGTH = 50;

    /** Column heading ({@code LABEL ON COLUMN ... IS}), shown as three lines of 20. */
    static final int MAX_HEADING_LENGTH = 60;

    private RemarksUtilDB2i() {
    }

    /**
     * Trims the remarks (null becomes an empty string, as in liquibase-core) and cuts them to {@code max}
     * characters without splitting a surrogate pair. Logs a warning if the text is truncated.
     */
    static String truncate(String remarks, int max) {
        String text = StringUtil.trimToEmpty(remarks);
        if (text.length() <= max) {
            return text;
        }
        int end = Character.isHighSurrogate(text.charAt(max - 1)) ? max - 1 : max;
        Scope.getCurrentScope().getLog(RemarksUtilDB2i.class).warning(
                "Remarks longer than " + max + " characters are truncated on DB2 for i: '" + text + "'");
        return text.substring(0, end);
    }
}
