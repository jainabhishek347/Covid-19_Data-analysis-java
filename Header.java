package com.XMLFILE2;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Header {


    public static void main(String[] args) throws SQLException, IOException, ParserConfigurationException, SAXException {
        String apiUrl = "https://data.cityofchicago.org/resource/553k-3xzc.json"; // Replace with your API URL
        List<String> headers = null;
        int limit = 50;
        int offset = 0;
        int count = 0;
        JsonObject jsonObject = null;
        JsonArray jsonArray = null;
        while (true) {
            try {
                String requestUrl = apiUrl + "?$limit=" + limit + "&$offset=" + offset;

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(requestUrl))
                        .GET()
                        .build();

                HttpClient client = HttpClient.newHttpClient();


                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                jsonArray = null;
                if (response.statusCode() == 200) {
                    String responseBody = response.body();
                    jsonArray = JsonParser.parseString(responseBody).getAsJsonArray();

                    if(jsonArray.size()==0)
                    {
                        break;
                    }

                    for (JsonElement element : jsonArray) {
                        jsonObject = element.getAsJsonObject();
                    }

                    // Extract headers from the first object in the array

                    JsonObject firstObject = (JsonObject) jsonArray.get(0);
                    headers = new ArrayList();
                    for (String key : firstObject.keySet()) {
                        headers.add(key);
                    }
                    offset += limit;
                }
            } catch (Exception e) {
                System.out.println(e);
            }// ...

            ForStaging staging = new ForStaging();
            staging.CreatTable(headers);
           staging.insertData(jsonObject, jsonArray);
            count+=limit;
        }
        System.out.println(count);
        InsertClass ic=new InsertClass();
        ic.insert();
        ic.updateQuery();
        ic.updated_data();
        ic.Is_deleted();

        Final_table ft=new Final_table();
        ft.final_insert();
        ft.update();
        ft.delete();
    }




}



