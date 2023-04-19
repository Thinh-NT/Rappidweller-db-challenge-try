## Challenge description: What to achieve

- Use the prepared **./postgres/init.sql** to create the challenges data structure ( for the quickstart you can use the docker-compose.yml we prepared for you, this is starting a Postgres container with the prepared init.sql script in case you have docker and docker-compose installed on your machine )

- Use Node.js, Python or Java in combination with JDBC driver ( **./lib/postgresql-42.3.5.jar** ) to get JDBC Metadata ( Tables, Columns, PrimaryKey, ForeignKey ... ) from a PostgresDB ( in python you can use JayDeBeApi for example )

The extracted metadata shall be used to generate an XML structure with the following pattern ( similar to our BENERATOR script ):

- a table should start with a **generate** node, the attribute **type** contains the table name as value
- inside this **generate** node we have subnodes **id**, **attribute** and **reference** with attribute **type** and the jdbc-datatype of the column as value
  - **id**: use id when the column is the primary key of this table
  - **reference**: use reference when the column is a foreign key and has a reference to the primary key of a different table ( like **role_id** in **db_user** table)
  
    **!! ATTENTION** make always sure the referenced table is printed first, in case of **db_user**, **db_role** must be printed before **db_user** table
  
  - **attribute**: use attribute if this is a general column with no specific like **first_name** in **db_user**

for the attribute type please use the following mapping:  

```python
type_map: dict = {
        "VARCHAR": "string",
        "VARBINARY": "binary",
        "TINYINT": "byte",
        "TIMESTAMP": "timestamp",
        "TIME": "date",
        "SMALLINT": "short",
        "REAL": "double",
        "NUMERIC": "double",
        "LONGVARCHAR": "string",
        "JAVA_OBJECT": "object",
        "INTEGER": "int",
        "FLOAT": "float",
        "DOUBLE": "double",
        "DECIMAL": "big_decimal",
        "DATE": "date",
        "CLOB": "string",
        "CHAR": "string",
        "BOOLEAN": "boolean",
        "BLOB": "binary",
        "BIT": "byte",
        "BINARY": "binary",
        "BIGINT": "big_integer",
    }
```

result should look like this

```xml
    <generate type="tablex">
        <id name="primarykey" type="integer"/> # if primary key -> id instead attribute
        <attribute name="column1" type="string"/>
        <attribute name="column2" type="big_decimal"/>
        <attribute name="column3" type="short"/>
    </generate>

    <generate type="tabley">
        <id name="primarykey" type="integer"/> # if primary key -> id instead attribute
        <attribute name="column1" type="string"/>
        <attribute name="column2" type="big_decimal"/>
        <attribute name="column3" type="short"/>
        <reference name="tablex_id" selector="select primarykey from tablex" distribution="random"/> # reference because column tablex_id is foreign key column from table x
    </generate>

    <generate type="tablez">
        <id name="primarykey" type="integer"/> # if primary key -> id instead attribute
        <attribute name="column1" type="string"/>
        <attribute name="column2" type="big_decimal"/>
        <attribute name="column3" type="short"/>
        <reference name="tabley_id" selector="select primarykey from tabley" distribution="random"/> # reference because column tabley_id is foreign key column from table y
    </generate>
```

### Sidenotes

- There are no specific constraints on how to solve this challenge, Python, Node.js or Java is the frame and it is up to you to fill this framework with your solution to achieve the goal.
- The estimated time for this challenge should not exceed 4 to 8 hours.

__© Copyright 2019-2023, rapiddweller GmbH. All rights reserved.__
