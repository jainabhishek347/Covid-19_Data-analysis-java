package com.XMLFILE2;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sun.jdi.Value;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.*;
import java.util.*;

public class ForStaging {
    Connection conn;
    Statement statement;
    public void CreatTable(List<String> header) {
        try {
             conn = DriverManager.getConnection
                    ("jdbc:postgresql://localhost:5432/keshav", "postgres", "k@123");
               Statement statement=conn.createStatement();
            conn.setAutoCommit(false); // Disable auto-commit

            StringBuilder createTableStatement = new StringBuilder("CREATE TABLE IF NOT EXISTS staging1 (");

            for (String columnNames : header) {
                createTableStatement.append("\"").append(columnNames).append("\"").append(" ").append("VARCHAR(100)").append(", ");
            }
            createTableStatement.delete(createTableStatement.length() - 2, createTableStatement.length()); // Remove the trailing comma and space
            createTableStatement.append(")");
            //Statement statement = conn.createStatement();
            System.out.println(createTableStatement.toString());
            statement.executeUpdate(createTableStatement.toString());
            System.out.println("success");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void insertData(JsonObject jsonObject,JsonArray jsonArray)
    {
        try {
            // Assuming you have a PostgreSQL database and a connection already established

            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/keshav", "postgres", "k@123");
            connection.setAutoCommit(false);
            String tableName = "staging1";

            for (JsonElement element : jsonArray) {
                jsonObject = element.getAsJsonObject();
                StringBuilder columns = new StringBuilder();
                StringBuilder placeholders = new StringBuilder();
                List<Object> parameterValues = new ArrayList<>();

                Iterator<String> keys = jsonObject.keySet().iterator();
                while (keys.hasNext()) {
                    String key = keys.next();
                    Object value = jsonObject.get(key);

                    columns.append("\"").append(key).append("\"").append(",");
                    placeholders.append("?,");
                    parameterValues.add(value);
                }
                columns.setLength(columns.length() - 1); // Remove the last comma
                placeholders.setLength(placeholders.length() - 1); // Remove the last comma

                String insertQuery = "INSERT INTO " + tableName + " (" + columns.toString() + ") VALUES ("
                        + placeholders.toString() + ")";

                System.out.println(insertQuery);
                PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

                // Bind parameter values
                for (int i = 0; i < parameterValues.size(); i++) {
                    Object value = parameterValues.get(i).toString().replaceAll(" ", "");
                    preparedStatement.setString(i + 1, value.toString().replaceAll("\"", ""));
                }

                preparedStatement.executeUpdate();

                StringBuilder updateQuery = new StringBuilder();
                updateQuery.append("UPDATE staging1 ");
                updateQuery.append("SET zip_code = NULL ");
                updateQuery.append("WHERE zip_code = 'Unknown';");
                statement=connection.createStatement();
                statement.executeUpdate(updateQuery.toString());
                connection.commit();


            }
        } catch (SQLException e) {
            e.printStackTrace();
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }finally {
            // Close the statement and connection
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}