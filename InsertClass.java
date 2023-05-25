package com.XMLFILE2;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertClass {
    File xmlFile = new File("mapping.xml");  // Replace with your XML file path
    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    Document document = documentBuilder.parse(xmlFile);

    // Get the root element of the XML document
    Element rootElement = document.getDocumentElement();
    String staging = "staging1";

    public InsertClass() throws ParserConfigurationException, IOException, SAXException {
    }

    public void insert() throws SQLException, IOException, SAXException, ParserConfigurationException {
        try {

            // Assuming you have a PostgreSQL database and a connection already established
            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/keshav", "postgres", "k@123");

            // Read the XML mapping file

            // Get the dim_table and query values from the XML mapping
            String dimTable = rootElement.getElementsByTagName("dim_table").item(0).getTextContent();
            String query = rootElement.getElementsByTagName("query").item(0).getTextContent();

            // Get the column mappings from the XML mapping
            NodeList columnMappings = rootElement.getElementsByTagName("column_mapping1");

            // Construct the INSERT INTO...SELECT query dynamically
            StringBuilder insertQuery = new StringBuilder();
            insertQuery.append("INSERT INTO ").append(dimTable).append(" (");

            for (int i = 0; i < columnMappings.getLength(); i++) {
                Element columnMapping = (Element) columnMappings.item(i);
                String dimColumn = columnMapping.getAttribute("dim_column");

                insertQuery.append(dimColumn);

                if (i < columnMappings.getLength() - 1) {
                    insertQuery.append(", ");
                }
            }

            insertQuery.append(")\n");
            insertQuery.append("SELECT ");

            for (int i = 0; i < columnMappings.getLength(); i++) {
                Element columnMapping = (Element) columnMappings.item(i);
                String sColumn = columnMapping.getAttribute("s_column");
                String dimType = columnMapping.getAttribute("data_type");

                if (dimType.equalsIgnoreCase("integer")) {
                    insertQuery.append("CASE WHEN TRIM(LOWER(s.").append(sColumn).append(")) ~ E'^\\\\d+$' THEN CAST(s.").append(sColumn).append(" AS INTEGER) ELSE NULL END");
                } else {
                    insertQuery.append("CAST(s.").append(sColumn).append(" AS ").append(dimType).append(")");
                }

                if (i < columnMappings.getLength() - 1) {
                    insertQuery.append(", ");
                }
            }

            insertQuery.append("\nFROM ").append(query).append(" s\n");
            insertQuery.append("WHERE  not exists \n(");
            insertQuery.append("select 1 from "+dimTable+" d");
            insertQuery.append(" where \n case when trim (LOWER(s.zip_code)) ~ E'^\\\\d+$' THEN CAST(s.zip_code AS INTEGER) ELSE 0 END = COALESCE(d.zip_code, 0)\n" +
                    "        AND CAST(s.date AS DATE) = d.date_m");
            insertQuery.append(");");

            // Print the generated SQL query
            System.out.println(insertQuery.toString());

            // Execute the SQL query
            Statement statement = connection.createStatement();
            int rowsAffected = statement.executeUpdate(insertQuery.toString());

            // Print the number of rows affected
            System.out.println(rowsAffected + " rows inserted.");

            // Close the resources
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateQuery() throws SQLException {
        try {
            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/keshav", "postgres", "k@123");
            String dimTable = rootElement.getElementsByTagName("dim_table").item(0).getTextContent();
            String query = rootElement.getElementsByTagName("query").item(0).getTextContent();

            // Get the column mappings from the XML mapping
            NodeList columnMappings = rootElement.getElementsByTagName("column_mapping1");

            // Construct the INSERT INTO...SELECT query dynamically
            StringBuilder updateQuery = new StringBuilder();
            updateQuery.append("update ").append(dimTable + "\n");
            updateQuery.append("set\n");
            updateQuery.append("is_active=0,\n");
            updateQuery.append("updated_at=now()\n");
            updateQuery.append("from " + staging + " s");
            updateQuery.append(" where coalesce(" + dimTable + ".zip_code,null )=coalesce( (CASE WHEN TRIM(LOWER(s.zip_code)) ~ E'^\\d+$' THEN CAST(s.zip_code AS INTEGER) ELSE null END ),0) \n");
            updateQuery.append("and " + dimTable + ".date_m=CAST(s.date AS DATE) ");
            updateQuery.append("  and(");
            for (int i = 2; i < columnMappings.getLength(); i++) {


                Element columnMapping = (Element) columnMappings.item(i);
                String dimColumn = columnMapping.getAttribute("dim_column");
                String sColumn = columnMapping.getAttribute("s_column");
                String dimType = columnMapping.getAttribute("data_type");

                if (dimType.equalsIgnoreCase("integer")) {
                    updateQuery.append(dimTable + "." + dimColumn + "<>CASE WHEN TRIM(LOWER(s.").append(sColumn).append(")) ~ E'^\\\\d+$' THEN CAST(s.").append(sColumn).append(" AS INTEGER) ELSE NULL END");
                } else {
                    updateQuery.append(dimTable + "." + dimColumn + "<>").append("CAST(s.").append(sColumn).append(" AS ").append(dimType).append(")");
                }

                updateQuery.append(" or \n");


            }
            updateQuery.setLength(updateQuery.length() - " OR\n".length());
            updateQuery.append(")");
            updateQuery.append("AND " + dimTable + ".is_active = 1 ;");
            System.out.println(updateQuery);
            Statement statement = connection.createStatement();
            int updateRows = statement.executeUpdate(updateQuery.toString());
            System.out.println("updated row=" + updateRows);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updated_data() {

        try {
            // Assuming you have a PostgreSQL database and a connection already established
            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/keshav", "postgres", "k@123");

            // Read the XML mapping file

            // Get the dim_table and query values from the XML mapping
            String dimTable = rootElement.getElementsByTagName("dim_table").item(0).getTextContent();
            String query = rootElement.getElementsByTagName("query").item(0).getTextContent();

            // Get the column mappings from the XML mapping
            NodeList columnMappings = rootElement.getElementsByTagName("column_mapping1");

            // Construct the INSERT INTO...SELECT query dynamically
            StringBuilder insertQuery1 = new StringBuilder();
            insertQuery1.append("INSERT INTO ").append(dimTable).append(" (");

            for (int i = 0; i < columnMappings.getLength(); i++) {
                Element columnMapping = (Element) columnMappings.item(i);
                String dimColumn = columnMapping.getAttribute("dim_column");

                insertQuery1.append(dimColumn);

                if (i < columnMappings.getLength() - 1) {
                    insertQuery1.append(", ");
                }
            }

            insertQuery1.append(")\n");
            insertQuery1.append("SELECT ");
            for (int i = 0; i < columnMappings.getLength(); i++) {
                Element columnMapping = (Element) columnMappings.item(i);
                String dimColumn = columnMapping.getAttribute("dim_column");

                String sColumn = columnMapping.getAttribute("s_column");
                String dimType = columnMapping.getAttribute("data_type");

                if (dimType.equalsIgnoreCase("integer")) {
                    insertQuery1.append("CASE WHEN TRIM(LOWER(s.").append(sColumn).append(")) ~ E'^\\\\d+$' THEN CAST(s.").append(sColumn).append(" AS INTEGER) ELSE NULL END");
                } else {
                    insertQuery1.append("CAST(s.").append(sColumn).append(" AS ").append(dimType).append(")");
                }

                if (i < columnMappings.getLength() - 1) {
                    insertQuery1.append(", ");
                }
            }

            insertQuery1.append("FROM ").append(staging).append(" s");
            insertQuery1.append(" LEFT JOIN ").append(dimTable).append(" d ON CASE WHEN TRIM(LOWER(s.zip_code)) ~ E'^\\\\d+$' THEN CAST(s.zip_code AS INTEGER) ELSE 0 END = COALESCE(d.zip_code, 0)");
            insertQuery1.append(" WHERE d.zip_code IS NOT NULL AND CAST(s.date AS DATE) = d.date_m");
            insertQuery1.append(" AND (");

            for (int i = 0; i < columnMappings.getLength(); i++) {
                Element columnMapping = (Element) columnMappings.item(i);
                String dimColumn = columnMapping.getAttribute("dim_column");
                String sColumn = columnMapping.getAttribute("s_column");
                String dimType = columnMapping.getAttribute("data_type");

                if (dimType.equalsIgnoreCase("integer")) {
                    insertQuery1.append("d.").append(dimColumn).append(" <> CASE WHEN TRIM(LOWER(s.").append(sColumn).append(")) ~ E'^\\\\d+$' THEN CAST(s.").append(sColumn).append(" AS INTEGER) ELSE NULL END");
                } else {
                    insertQuery1.append("d.").append(dimColumn).append(" <> ").append("CAST(s.").append(sColumn).append(" AS ").append(dimType).append(")");
                }

                if (i < columnMappings.getLength() - 1) {
                    insertQuery1.append(" OR ");
                }
            }

            insertQuery1.append(")");
            insertQuery1.append(" AND d.updated_at = (SELECT MAX(updated_at) FROM ").append(dimTable).append(" WHERE d.zip_code = ").append(dimTable).append(".zip_code)");
            insertQuery1.append(" AND d.date_m = ").append("d.date_m;");
            System.out.println(insertQuery1);
            Statement statement=connection.createStatement();
            int inserted=statement.executeUpdate(insertQuery1.toString());
            System.out.println("updated data"+inserted);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        }

        public void Is_deleted() throws SQLException {
            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/keshav", "postgres", "k@123");

            // Read the XML mapping file

            // Get the dim_table and query values from the XML mapping
            String dimTable = rootElement.getElementsByTagName("dim_table").item(0).getTextContent();
            String query = rootElement.getElementsByTagName("query").item(0).getTextContent();

            // Get the column mappings from the XML mapping
            NodeList columnMappings = rootElement.getElementsByTagName("column_mapping1");

            StringBuilder deleteQuery=new StringBuilder();
            deleteQuery.append("update "+dimTable);
            deleteQuery.append("\nset \n");
            deleteQuery.append("is_active=0,\n");
            deleteQuery.append("updated_at=now() \n");
            deleteQuery.append("WHERE is_active=1 AND not exists ( select 1 from "+staging+" where "+dimTable+".zip_code = CASE WHEN TRIM(LOWER("+staging+".zip_code)) ~ E'^\\\\d+$' THEN CAST(staging1.zip_code AS INTEGER) ELSE 0 END ");
            deleteQuery.append(" AND "+dimTable+".date_m=cast("+staging+".date as date)");
            deleteQuery.append(");");
            Statement statement=connection.createStatement();
            System.out.println(deleteQuery);
            int delete=statement.executeUpdate(deleteQuery.toString());
            System.out.println("deleted="+delete);
        }


}

