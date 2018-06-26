package com.oner;


import com.microsoft.azure.documentdb.*;
import com.oner.model.BiTemporalDoc;


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

        String collectionLink = String.format("/dbs/%s/colls/%s", "ApiTest", "Instrument");
        client
                .queryDocuments(
                        collectionLink,
                        "SELECT top 100 * FROM r",
                        feedOptions).getQueryIterable().forEach(
                d -> createInstance(d)
                //{}

        );


    }

    private static void createInstance(Document d) {
        System.out.println
                (

                        String.format("Description: %s, TransactionTime: %s, ValidityRange: %s, AxiomaDataId: %s, Source: %s, MaturityDate: %s, Currency: %s, CurrentCoupon: %.2f",
                                d.get("Description"), d.get("TransactionTime"),
                                d.get("ValidityRange"), d.get("AxiomaDataId"), d.get("Source"), d.getString("MaturityDate"),
                                d.get("Currency"), d.getDouble("CurrentCoupon")
                        )

                );

    }

    static void readDataInstrumentCorrection(DocumentClient client) throws InterruptedException {

        FeedOptions feedOptions = new FeedOptions();
        feedOptions.setEnableCrossPartitionQuery(true);

        String collectionLink = String.format("/dbs/%s/colls/%s", "ApiTest", "InstrumentCorrection");
        client
                .queryDocuments(
                        collectionLink,
                        "SELECT top 100 * FROM r",
                        feedOptions).getQueryIterable().forEach(
                                d -> System.out.println
                                (
                                    String.format("TransactionTime: %s, ValidityRange: %s, AxiomaDataId: %s, Source: %s, MaturityDate: %s",
                                        d.get("TransactionTime"),
                                        d.get("ValidityRange"), d.get("AxiomaDataId"), d.get("Source"), d.getString("MaturityDate")
                                    )
                                )
                                //System.out.println(d)

        //{}

                );




    }
}
