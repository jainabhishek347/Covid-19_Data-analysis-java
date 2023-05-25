package com.XMLFILE2;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

public class Final_table {
    File xmlFile = new File("mapping.xml");  // Replace with your XML file path
    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();


    Document document = documentBuilder.parse(xmlFile);

    // Get the root element of the XML document
    Element rootElement = document.getDocumentElement();
    String staging = "staging1";
    Connection connection;

    public Final_table() throws ParserConfigurationException, IOException, SAXException {

    }

    public void final_insert() {

        try {
            connection = DriverManager.getConnection
                    ("jdbc:postgresql://localhost:5432/keshav", "postgres", "k@123");

            String final_table = rootElement.getElementsByTagName("final_table").item(0).getTextContent();
            String staging = rootElement.getElementsByTagName("staging").item(0).getTextContent();
            NodeList columnMappings = rootElement.getElementsByTagName("column_mapping2");
            StringBuilder insertQuery = new StringBuilder();
            insertQuery.append("insert into ").append(final_table).append(" (");
            StringBuilder updateQuery = new StringBuilder();

            updateQuery.append(final_table);


            for (int i = 0; i < columnMappings.getLength(); i++) {
                Element columnMapping = (Element) columnMappings.item(i);
                String final_column = columnMapping.getAttribute("final_column");
                insertQuery.append(final_column);
                if (i < columnMappings.getLength() - 1) {
                    insertQuery.append(", ");

                }

            }
            insertQuery.append(" )");
            insertQuery.append(" select ");

            for (int i = 0; i < columnMappings.getLength(); i++) {
                Element columnMapping = (Element) columnMappings.item(i);
                String final_column = columnMapping.getAttribute("final_column");
                String sColumn = columnMapping.getAttribute("s_column");
                String finalType = columnMapping.getAttribute("data_type");


                if (finalType.equalsIgnoreCase("INTEGER")) {
                    insertQuery.append("case when trim(lower (s.").append(sColumn).append(" ) ) ~ E'^\\\\d+$' then cast(s.").append(sColumn).append(" as INTEGER) else null end");
                } else {
                    insertQuery.append("cast(s.").append(sColumn).append(" as ").append(finalType).append(" )");
                }
                if (i < columnMappings.getLength() - 1) {
                    insertQuery.append(", ");

                }
            }
            insertQuery.append("\nfrom ").append(staging).append(" s\n");
            insertQuery.append("where not exists \n(");
            insertQuery.append("select 1 from " + final_table + " d ");
            insertQuery.append("where \n case when trim(lower(s.zip_code)) ~ E'^\\\\d+$' then cast(s.zip_code as INTEGER) else 0 " +
                    "end = coalesce(d.zip_code_f,0) \n and  cast(s.date as date )= d.date_f");
            insertQuery.append(" );");
            System.out.println(insertQuery);
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);
            int insert_rows = statement.executeUpdate(insertQuery.toString());
            connection.commit();
            System.out.println("Inserted rows = " + insert_rows);
        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ee) {
                    ee.printStackTrace();
                }
            }
        }


    }

    public void update() {

        StringBuilder updateQuery = new StringBuilder();
        String final_table = rootElement.getElementsByTagName("final_table").item(0).getTextContent();

        String staging = rootElement.getElementsByTagName("staging").item(0).getTextContent();
        NodeList colunmMappings = rootElement.getElementsByTagName("column_mapping2");
        updateQuery.append("UPDATE \n");
        updateQuery.append(final_table + " f \n");
        updateQuery.append(" set ");
        for (int i = 0; i < colunmMappings.getLength(); i++) {
            Element columnMapping = (Element) colunmMappings.item(i);
            String fcolumn = columnMapping.getAttribute("final_column");
            String sColumn = columnMapping.getAttribute("s_column");
            String finalType = columnMapping.getAttribute("data_type");
            if (finalType.equalsIgnoreCase("INTEGER")) {
                updateQuery.append(fcolumn + " = ").append("case when trim(lower (s.").append(sColumn).append(" ) ) ~ E'^\\\\d+$' then cast(s.").append(sColumn).append(" as INTEGER) else null end");
            } else {
                updateQuery.append(fcolumn + " = ").append("cast(s.").append(sColumn).append(" as ").append(finalType).append(" )");
            }
            if (i < colunmMappings.getLength() - 1) {
                updateQuery.append(", ");
            }


        }
        updateQuery.append(" from " + staging + " s");
        updateQuery.append(" where case when trim(lower(s.zip_code)) ~ E'^\\\\d+$' then cast(s.zip_code as INTEGER) else 0  \n" +
                " end = coalesce(f.zip_code_f,0)  and  cast(s.date as date )= f.date_f and (");


        for (int i = 2; i < colunmMappings.getLength(); i++) {


            Element columnMapping = (Element) colunmMappings.item(i);
            String final_column = columnMapping.getAttribute("final_column");
            String sColumn = columnMapping.getAttribute("s_column");
            String dimType = columnMapping.getAttribute("data_type");

            if (dimType.equalsIgnoreCase("integer")) {
                updateQuery.append("f." + final_column + "<>CASE WHEN TRIM(LOWER(s.").append(sColumn).append(")) ~ E'^\\\\d+$' THEN CAST(s.").append(sColumn).append(" AS INTEGER) ELSE NULL END");
            } else {
                updateQuery.append("f." + final_column + "<>").append("CAST(s.").append(sColumn).append(" AS ").append(dimType).append(")");
            }

            updateQuery.append(" or \n");


        }
        updateQuery.setLength(updateQuery.length() - " OR\n".length());
        updateQuery.append(" )");
        System.out.println(updateQuery);
        try {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            int updated = statement.executeUpdate(updateQuery.toString());
            System.out.println("updated rows " + updated);
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ee) {
                    ee.printStackTrace();
                }
            }
        }
    }
    public void delete()
    {
        String final_table = rootElement.getElementsByTagName("final_table").item(0).getTextContent();
        StringBuilder delete=new StringBuilder();
        delete.append("update \n");
        delete.append(final_table+ " f\n");
        delete.append(" set \n");
        delete.append("is_deleted =1,");
        delete.append("is_active = 0 ,");
        delete.append("updated_at = now()");
        delete.append(" where is_active =1 and not exists (select 1 from "+staging+" s  where case when trim(lower(s.zip_code)) ~ E'^\\d+$' then cast(s.zip_code as INTEGER) else 0  " +
                "  end = coalesce(f.zip_code_f,0)  and  cast(s.date as date )= f.date_f )");
        StringBuilder truncate=new StringBuilder();
        truncate.append("truncate ").append(staging+" ;");

        System.out.println(delete);
        try {
            Statement statement = connection.createStatement();
            int deleted=statement.executeUpdate(delete.toString());
            statement.executeUpdate(truncate.toString());

            System.out.println("numbers of deleted records = ");
            System.out.println(" staging table truncated ");
            connection.commit();
        }
        catch (SQLException e) {
            e.printStackTrace();
            if(connection!=null)
            {
                try
                {
                    connection.rollback();
                }catch (SQLException ee)
                {
                    ee.printStackTrace();
                }
            }
        }
        finally
        {
            if(connection!=null)
            {
                try {
                    connection.close();
                }
                catch (SQLException ex)
                {
                    ex.printStackTrace();
                }

            }
        }
    }
}
