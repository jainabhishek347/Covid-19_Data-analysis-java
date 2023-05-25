## XMLFILE2

This Java program retrieves data from an API and performs various operations on the data.

## Description

The `Header` class serves as the entry point for the program. It makes use of the `HttpClient` class to send HTTP requests and fetch data from the specified API endpoint.

The program follows these steps:
1. Define the API URL and initialize variables.
2. Enter a loop to fetch data in batches until all data is retrieved.
3. Send an HTTP GET request to the API URL with limit and offset parameters.
4. Parse the response JSON into a `JsonArray` using the `JsonParser` class.
5. Extract the headers from the first object in the array.
6. Perform data processing or insert the data into a staging table using the `ForStaging` class.
7. Update the offset and check if there is more data to fetch.
8. Repeat steps 3-7 until all data is retrieved.
9. Perform various operations using other classes like `InsertClass` and `Final_table`.
10. creating xml file to configure colunms of staging table ,diension table and final dimension table

## Prerequisites

- Java Development Kit (JDK)
- Add Gson Depandency in your pom.xml file Gson library for JSON parsing
- HttpClient library for making HTTP requests
- Add postgresql dependency inpom.xml
## Usage

To use this program:
1. Set the `apiUrl` variable to the desired API endpoint.2. Make sure the required dependencies are included in the classpath.
3. Compile and run the `Header` class.

 
# ForStaging

The `ForStaging` class provides functionality for creating a database table and inserting data into it. It is designed to work with PostgreSQL databases and uses JDBC for database connectivity.

To create a table in the database, use the `CreatTable` method in the `ForStaging` class. Pass a list of column names as the parameter.


ForStaging forStaging = new ForStaging();
List<String> columnNames = Arrays.asList();
forStaging.CreatTable(object of list);
Create staging table dynamiclly. 

# Inserting Data

# XML Mapping Configuration

This repository contains XML mapping configuration for dimension table

## xml Java Code Explanation

 how to read an XML file and extract information from it.

### Code Description

1. The XML file path is specified as `mapping.xml`. Make sure to replace it with the actual path to your XML file.

2. The code uses the `DocumentBuilderFactory` and `DocumentBuilder` classes to parse the XML file and obtain a `Document` object.

3. The root element of the XML document is retrieved using `getDocumentElement()` method.

4. The following variables are extracted from the XML:
   - `staging`: Represents the staging table name, extracted from the `<query>` tag in the XML.
   - `dimTable`: Represents the dimension table name, extracted from the `<dim_table>` tag in the XML.

5. The column mappings are extracted from the XML using `getElementsByTagName()` method and a loop is used to iterate over each `<column_mapping1>` element.

6. Within the loop, the following information is extracted for each column mapping:
   - `dimColumn`: Represents the dimension column, extracted from the `dim_column` attribute of the `<column_mapping1>` element.

7. The extracted information can be used to construct SQL queries or perform further processing.

To insert data into the table, use the insertData method in the ForStaging class. Pass a JsonObject and a JsonArray containing the data to be inserted.

ForStaging forStaging = new ForStaging();
JsonObject jsonObject = new JsonObject();
JsonArray jsonArray = new JsonArray();

// Add data to the JsonObject and JsonArray

forStaging.insertData(jsonObject, jsonArray);

# InsertClass

The `InsertClass` is a Java class that provides functionality for inserting and updating data in a PostgreSQL database based on an XML mapping file.

## Requirements

To use the `InsertClass`, you need the following:

- Java Development Kit (JDK)
- PostgreSQL database
- JDBC driver for PostgreSQL (e.g., `postgresql`)
- XML mapping file (e.g., `mapping.xml`)

## Usage

1. Create an XML mapping file (`mapping.xml`) that specifies the database table, query, and column mappings.
2. Import the `com.XMLFILE2` package into your project.
3. Create an instance of the `InsertClass` and call the appropriate methods to insert or update data in the database.


# XML Mapping Configuration

This repository contains XML mapping configuration for dimension table and final table.

## Dimension Table Mapping

- **Dimension Table Name**: my_dimension_table
- **Staging Table Name**: staging1

### Column Mappings

- **zip_code**: Maps to the `zip_code` column in the staging table. Data type: INT
- **date_m**: Maps to the `date` column in the staging table. Data type: DATE
- **daily_dose**: Maps to the `total_doses_daily` column in the staging table. Data type: INTEGER
- **first_dose**: Maps to the `_1st_dose_daily` column in the staging table. Data type: INTEGER
- **zip_loc**: Maps to the `zip_code_location` column in the staging table. Data type: VARCHAR

## Final Table Mapping

- **Final Table Name**: final_table
- **Staging Table Name**: staging1

### Column Mappings

- **zip_code_f**: Maps to the `zip_code` column in the staging table. Data type: INTEGER
- **date_f**: Maps to the `date` column in the staging table. Data type: DATE
- **daily_dose_f**: Maps to the `total_doses_daily` column in the staging table. Data type: INTEGER
- **first_dose_f**: Maps to the `_1st_dose_daily` column in the staging table. Data type: INTEGER
- **zip_loc_f**: Maps to the `zip_code_location` column in the staging table. Data type: VARCHAR

### Inserting Data

To insert data into the database, use the `insert()` method in the `InsertClass`. This method reads the XML mapping file and constructs an SQL query to insert data into the specified table.
InsertClass insertClass = new InsertClass();
insertClass.insert();

Updating Data
To update data in the database, use the updateQuery() method in the InsertClass. This method reads the XML mapping file and constructs an SQL query to update the specified table based on certain conditions.

InsertClass insertClass = new InsertClass();
insertClass.updateQuery();

Updating Modified Data
To update only the modified data in the database, use the updated_data() method in the InsertClass. This method reads the XML mapping file and constructs an SQL query to insert updated data into the specified table based on certain conditions.


Marking Deleted Data
To mark deleted data in the database as inactive, use the Is_deleted() method in the InsertClass. This method reads the XML mapping file and constructs an SQL query to update the specified table and mark the deleted data as inactive.

InsertClass insertClass = new InsertClass();
insertClass.Is_deleted();

# XML Mapping Configuration - Final Table

This repository contains Java code that reads an XML file and performs operations on a final table based on the mapping configuration.

## Java Code Explanation

The provided Java code performs the following operations:

### 1. Insert Data into Final Table

The `final_insert()` method inserts data into the final table based on the XML mapping configuration. The code performs the following steps:

- Establishes a connection to a PostgreSQL database.
- Retrieves the final table name and staging table name from the XML file.
- Retrieves the column mappings from the XML file and constructs an INSERT query to insert data into the final table.
- Executes the INSERT query and commits the transaction.

### 2. Update Data in Final Table

The `update()` method updates data in the final table based on the XML mapping configuration. The code performs the following steps:

- Constructs an UPDATE query to update data in the final table based on the column mappings and conditions specified in the XML file.
- Executes the UPDATE query and commits the transaction.

### 3. Delete Data from Final Table

The `delete()` method deletes data from the final table based on the XML mapping configuration. The code performs the following steps:

- Constructs an UPDATE query to mark records in the final table as deleted based on the conditions specified in the XML file.
- Truncates the staging table.
- Executes the UPDATE query and truncation query, and commits the transaction.

Feel free to modify the code according to your specific requirements and XML structure.

## Prerequisites
- Adjust the database connection details in the code (`jdbc:postgresql://localhost:5432/keshav`, "postgres", "k@123") to match your database configuration.

