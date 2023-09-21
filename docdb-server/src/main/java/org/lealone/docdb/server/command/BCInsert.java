/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.db.value.ValueString;
import org.lealone.docdb.server.DocDBServerConnection;

public class BCInsert extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn) {
        Table table = getTable(doc, "insert");
        ArrayList<BsonDocument> list = new ArrayList<>();
        ServerSession session = createSession(table.getDatabase());
        BsonArray documents = doc.getArray("documents", null);
        if (documents != null) {
            for (int i = 0, size = documents.size(); i < size; i++) {
                list.add(documents.get(i).asDocument());
            }
        }
        // mongodb-driver-sync会把documents包含在独立的payload中，需要特殊处理
        if (input.hasRemaining()) {
            input.readByte();
            input.readInt32(); // size
            input.readCString();
            while (input.hasRemaining()) {
                doc = conn.decode(input);
                list.add(doc);
            }
        }
        int size = list.size();
        AtomicInteger counter = new AtomicInteger(size);
        AtomicBoolean isFailed = new AtomicBoolean(false);
        for (int i = 0; i < size && !isFailed.get(); i++) {
            String json = list.get(i).toJson();
            if (DEBUG)
                logger.info(doc.toJson());
            Row row = table.getTemplateRow();
            row.setValue(0, ValueString.get(json));
            table.addRow(session, row).onComplete(ar -> {
                if (isFailed.get())
                    return;
                if (ar.isFailed()) {
                    isFailed.set(true);
                    session.rollback();
                }
                if (counter.decrementAndGet() == 0 || isFailed.get()) {
                    session.commit();
                    session.close();
                }
            });
        }
        BsonDocument document = new BsonDocument();
        setOk(document);
        setN(document, size);
        return document;
    }
}
