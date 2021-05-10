# Liquibase DB2 iSeries Extension [![Build and Test Extension](https://github.com/liquibase/liquibase-db2i/actions/workflows/build.yml/badge.svg)](https://github.com/liquibase/liquibase-db2i/actions/workflows/build.yml)

This is a Liquibase DB2 extension for iSeries support. [DB2 iSeries](https://www.ibm.com/products/db2-database) enables Liquibase to connect to DB2 iSeries data sources. 

## Configuring the extension

These instructions will help you get the extension up and running on your local machine for development and testing purposes. This extension has a prerequisite of Liquibase core in order to use it. Liquibase core can be found at https://www.liquibase.org/download.

### Liquibase CLI

Download [the latest released Liquibase extension](https://github.com/liquibase/liquibase-db2i/releases) `.jar` file and place it in the `liquibase/lib` install directory. If you want to use another location, specify the extension `.jar` file in the `classpath` of your [liquibase.properties file](https://docs.liquibase.com/workflows/liquibase-community/creating-config-properties.html).

### Maven
Specify the Liquibase extension in the `<dependency>` section of your POM file by adding the `org.liquibase.ext` dependency for the Liquibase plugin. 
 
```  
<plugin>
     <!--start with basic information to get Liquibase plugin:
     include <groupId>, <artifactID>, and <version> elements-->
     <groupId>org.liquibase</groupId>
     <artifactId>liquibase-maven-plugin</artifactId>
     <version>4.3.2</version>
     <configuration>
        <!--set values for Liquibase properties and settings
        for example, the location of a properties file to use-->
        <propertyFile>liquibase.properties</propertyFile>
     </configuration>
     <dependencies>
     <!--set up any dependencies for Liquibase to function in your
     environment for example, a database-specific plugin-->
            <dependency>
                 <groupId>org.liquibase.ext</groupId>
                 <artifactId>liquibase-db2i</artifactId>
                 <version>${liquibase-db2i.version}</version>
            </dependency>
         </dependencies>
      </plugin>
  ``` 
  
## Java call
  
```
public class Application {
    public static void main(String[] args) {
        DB2iDatabase database = (DB2iDatabase) DatabaseFactory.getInstance().openDatabase(url, null, null, null, null);
        Liquibase liquibase = new Liquibase("liquibase/ext/changelog.generic.test.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");
    }
}
```
## Contribution

To file a bug, improve documentation, or contribute code, follow our [guidelines for contributing](https://www.liquibase.org/community). 

[This step-by-step instructions](https://www.liquibase.org/community/contribute/code) will help you contribute code for the extension. 

Once you have created a PR for this extension you can find the artifact for your build using the following link: [https://github.com/liquibase/liquibase-db2i/actions/workflows/build.yml](https://github.com/liquibase/liquibase-db2i/actions/workflows/build.yml).


## Documentation

[Using Liquibase with DB2 LUW](https://docs.liquibase.com/workflows/database-setup-tutorials/db2-luw.html)

## Issue Tracking

Any issues can be logged in the [Github issue tracker](https://github.com/liquibase/liquibase-db2i/issues).

## License

This project is licensed under the [Apache License Version 2.0](https://www.apache.org/licenses/LICENSE-2.0.html).
