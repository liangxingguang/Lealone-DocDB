/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.index.Cursor;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;

public class BCFind extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc) {
        Table table = getTable(doc, "find");
        ServerSession session = createSession(table.getDatabase());
        // Select select = new Select(session);
        BsonDocument filter = doc.getDocument("filter", null);
        if (filter != null) {
            if (DEBUG)
                logger.info("filter: {}", filter.toJson());
            // filter.forEach((k, v) -> {
            // Expression left = ValueExpression.get(ValueString.get(k));
            // Expression right = ValueExpression.get(ValueString.get(v.toString()));
            // Comparison cond = new Comparison(session, Comparison.EQUAL, left, right);
            // select.addCondition(cond);
            // });
        }
        BsonArray documents = new BsonArray();
        try {
            Cursor cursor = table.getScanIndex(session).find(session, null, null);
            while (cursor.next()) {
                String json = cursor.get().getValue(0).getString();
                BsonDocument document = BsonDocument.parse(json);
                if (filter != null) {
                    boolean b = true;
                    for (Entry<String, BsonValue> e : filter.entrySet()) {
                        BsonValue v = document.get(e.getKey());
                        if (v == null || !v.equals(e.getValue())) {
                            b = false;
                            break;
                        }
                    }
                    if (b) {
                        documents.add(document);
                    }
                } else {
                    documents.add(document);
                }
            }
            session.commit();
        } finally {
            session.close();
        }
        BsonDocument document = new BsonDocument();
        BsonDocument cursor = new BsonDocument();
        cursor.append("id", new BsonInt64(0));
        cursor.append("ns", new BsonString(
                doc.getString("$db").getValue() + "." + doc.getString("find").getValue()));
        cursor.append("firstBatch", documents);
        document.append("cursor", cursor);
        setOk(document);
        return document;
    }
}
