/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.test;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.InsertOneResult;

public class DocDBCrudTest {

    public static void main(String[] args) {
        int port = 9610;
        // port = 27017;
        String connectionString = "mongodb://127.0.0.1:" + port + "/?serverSelectionTimeoutMS=200000";
        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase("database1");
        // System.out.println(database.runCommand(Document.parse("{\"buildInfo\": 1}")));
        // database.createCollection("collection2");
        MongoCollection<Document> collection = database.getCollection("collection1");
        insert(collection);
        query(collection);
        mongoClient.close();
    }

    static void insert(MongoCollection<Document> collection) {
        Document doc0 = new Document().append("f1", 2).append("f2", 1);
        InsertOneResult r = collection.insertOne(doc0);
        System.out.println("InsertedId: " + r.getInsertedId());
        doc0 = new Document().append("f1", 2).append("f2", 1);
        r = collection.insertOne(doc0);
        System.out.println("InsertedId: " + r.getInsertedId());
        // long count = collection.countDocuments();
        // System.out.println("total document count: " + count);
        // Document doc1 = new Document("_id", count + 1).append("f1", 1).append("f2", 1);
        // collection.insertOne(doc1);
    }

    static void query(MongoCollection<Document> collection) {
        MongoCursor<Document> cursor = collection.find(Filters.eq("f1", 2))
                .projection(Projections.include("f2")).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
    }
}
