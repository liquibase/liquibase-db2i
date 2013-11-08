package liquibase.ext.db2i.database;

import liquibase.sdk.supplier.database.ConnectionConfiguration;

public class DB2iConfigStandard extends ConnectionConfiguration {
    @Override
    public String getDatabaseShortName() {
        return "db2i";
    }

    @Override
    public String getConfigurationName() {
        return NAME_STANDARD;
    }

    @Override
    public String getUrl() {
        return "jdbc:as400:TODO";
    }
}
