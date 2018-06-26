package com.oner;


import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;

import java.util.List;


public class AzureSource {

    public static void main(String[] args) throws Exception{
        try(DocumentClient client = new DocumentClient("https://instrumentpoc.documents.azure.com:443/",
                "fsvNaYuSWHlDkosSqu0OxkUlMbJZ7Qwfi5EWKlJ5LJ7F2pq42V8rWctIuciScqbewOeA9cHN5XuAbBHUd4TD5g==", ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session)) {

            readData(client);
        }
    }


    static void readData(DocumentClient client) throws InterruptedException {

        FeedOptions feedOptions = new FeedOptions();
        feedOptions.setEnableCrossPartitionQuery(true);

        String collectionLink = String.format("/dbs/%s/colls/%s", "ApiTest", "InstrumentCorrection");
        client
                .queryDocuments(
                        collectionLink,
                        "SELECT * FROM r",
                        feedOptions).getQueryIterable().forEach(d -> {

                });

    }
}
