/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;

public class BCAggregate extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc) {
        Table table = getTable(doc, "aggregate");
        BsonArray pipeline = doc.getArray("pipeline", null);
        long rowCount = 0;
        if (pipeline != null) {
            for (int i = 0, size = pipeline.size(); i < size; i++) {
                BsonDocument document = pipeline.get(i).asDocument();
                BsonDocument group = document.getDocument("$group", null);
                if (group != null) {
                    BsonDocument agg = group.getDocument("n", null);
                    if (agg != null && agg.containsKey("$sum")) {
                        ServerSession session = createSession(table.getDatabase());
                        rowCount = table.getRowCount(session);
                        session.close();
                        break;
                    }
                }
            }
        }
        BsonDocument document = new BsonDocument();
        BsonDocument cursor = new BsonDocument();
        cursor.append("id", new BsonInt64(0));
        cursor.append("ns", new BsonString(
                doc.getString("$db").getValue() + "." + doc.getString("aggregate").getValue()));
        BsonArray documents = new BsonArray();
        BsonDocument agg = new BsonDocument();
        agg.append("_id", new BsonInt32(1));
        agg.append("n", new BsonInt32((int) rowCount));
        documents.add(agg);
        cursor.append("firstBatch", documents);
        document.append("cursor", cursor);
        setOk(document);
        return document;
    }
}
