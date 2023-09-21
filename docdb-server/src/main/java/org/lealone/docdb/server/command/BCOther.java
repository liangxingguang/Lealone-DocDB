/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.docdb.server.DocDBServerConnection;

public class BCOther extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn, String command) {
        switch (command) {
        case "hello":
        case "ismaster": {
            BsonDocument document = new BsonDocument();
            document.append("ismaster", new BsonBoolean(true));
            document.append("connectionId", new BsonInt32(conn.getConnectionId()));
            document.append("readOnly", new BsonBoolean(false));
            setWireVersion(document);
            setOk(document);
            document.append("isWritablePrimary", new BsonBoolean(true));
            return document;
        }
        case "buildinfo": {
            BsonDocument document = new BsonDocument();
            document.append("version", new BsonString("6.0.0"));
            setOk(document);
            return document;
        }
        case "getparameter": {
            BsonDocument document = new BsonDocument();
            BsonDocument v = new BsonDocument();
            v.append("version", new BsonString("6.0"));
            document.append("featureCompatibilityVersion", v);
            setOk(document);
            return document;
        }
        case "drop": {
            Table table = getTable(doc, "drop");
            try (ServerSession session = createSession(table.getDatabase())) {
                String sql = "DROP TABLE IF EXISTS " + table.getSQL();
                session.prepareStatementLocal(sql).executeUpdate();
            }
            BsonDocument document = new BsonDocument();
            setOk(document);
            return document;
        }
        case "startsession": {
            Database db = getDatabase(doc);
            ServerSession session = createSession(db);
            UUID id = UUID.randomUUID();
            conn.getSessions().put(id, session);
            BsonDocument document = new BsonDocument();
            document.append("id", new BsonBinary(id));
            document.append("timeoutMinutes", new BsonInt32(30));
            setOk(document);
            return document;
        }
        case "killsessions": {
            for (UUID id : decodeUUIDs(doc, "killSessions")) {
                ServerSession session = conn.getSessions().remove(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        case "refreshsessions": {
            for (UUID id : decodeUUIDs(doc, "refreshSessions")) {
                ServerSession session = conn.getSessions().get(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        case "endsessions": {
            for (UUID id : decodeUUIDs(doc, "endSessions")) {
                ServerSession session = conn.getSessions().remove(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        default:
            BsonDocument document = new BsonDocument();
            setWireVersion(document);
            setOk(document);
            setN(document, 0);
            return document;
        }
    }

    private static List<UUID> decodeUUIDs(BsonDocument doc, Object key) {
        BsonArray ba = doc.getArray(key, null);
        if (ba != null) {
            int size = ba.size();
            List<UUID> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                BsonDocument s = ba.get(i).asDocument();
                UUID id = s.getBinary("id").asUuid();
                list.add(id);
            }
            return list;
        }
        return Collections.emptyList();
    }
}
