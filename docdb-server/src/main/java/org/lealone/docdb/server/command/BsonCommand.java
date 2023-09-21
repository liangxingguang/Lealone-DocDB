/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.auth.User;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.docdb.server.DocDBServer;

public abstract class BsonCommand {

    public static final Logger logger = LoggerFactory.getLogger(BsonCommand.class);
    public static final boolean DEBUG = true;

    public static BsonDocument newOkBsonDocument() {
        BsonDocument document = new BsonDocument();
        setOk(document);
        return document;
    }

    public static void setOk(BsonDocument doc) {
        doc.append("ok", new BsonInt32(1));
    }

    public static void setN(BsonDocument doc, int n) {
        doc.append("n", new BsonInt32(n));
    }

    public static void setWireVersion(BsonDocument doc) {
        doc.append("minWireVersion", new BsonInt32(0));
        doc.append("maxWireVersion", new BsonInt32(17));
    }

    public static Database getDatabase(BsonDocument doc) {
        String dbName = doc.getString("$db").getValue();
        if (dbName == null)
            dbName = DocDBServer.DATABASE_NAME;
        Database db = LealoneDatabase.getInstance().findDatabase(dbName);
        if (db == null) {
            String sql = "CREATE DATABASE IF NOT EXISTS " + dbName;
            LealoneDatabase.getInstance().getSystemSession().prepareStatementLocal(sql).executeUpdate();
            db = LealoneDatabase.getInstance().getDatabase(dbName);
        }
        if (!db.isInitialized())
            db.init();
        return db;
    }

    public static Table getTable(BsonDocument doc, String key) {
        Database db = getDatabase(doc);
        Schema schema = db.getSchema(null, Constants.SCHEMA_MAIN);
        String tableName = doc.getString(key).getValue();
        Table table = schema.findTableOrView(null, tableName);
        if (table == null) {
            try (ServerSession session = createSession(db)) {
                String sql = "CREATE TABLE IF NOT EXISTS " + Constants.SCHEMA_MAIN + "." + tableName
                        + "(f varchar)";
                session.prepareStatementLocal(sql).executeUpdate();
            }
        }
        return schema.getTableOrView(null, tableName);
    }

    public static ServerSession createSession(Database db) {
        return db.createSession(getUser(db));
    }

    private static User getUser(Database db) {
        for (User user : db.getAllUsers()) {
            if (user.isAdmin())
                return user;
        }
        return db.getAllUsers().get(0);
    }
}
