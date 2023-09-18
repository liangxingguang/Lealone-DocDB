/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.auth.User;
import org.lealone.db.index.Cursor;
import org.lealone.db.result.Row;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.db.value.ValueString;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetBuffer;
import org.lealone.net.NetBufferOutputStream;
import org.lealone.net.WritableChannel;
import org.lealone.server.Scheduler;

public class DocDBServerConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(DocDBServerConnection.class);

    private final Scheduler scheduler;
    private final int connectionId;

    protected DocDBServerConnection(DocDBServer server, WritableChannel channel, Scheduler scheduler,
            int connectionId) {
        super(channel, true);
        this.scheduler = scheduler;
        this.connectionId = connectionId;
    }

    private void sendErrorMessage(Throwable e) {
    }

    private void sendMessage(byte[] data) {
        try (NetBufferOutputStream out = new NetBufferOutputStream(writableChannel, data.length,
                scheduler.getDataBufferFactory())) {
            out.write(data);
            out.flush(false);
        } catch (IOException e) {
            logger.error("Failed to send message", e);
        }
    }

    private final ByteBuffer packetLengthByteBuffer = ByteBuffer.allocateDirect(4);

    @Override
    public ByteBuffer getPacketLengthByteBuffer() {
        return packetLengthByteBuffer;
    }

    @Override
    public int getPacketLength() {
        int length = (packetLengthByteBuffer.get() & 0xff);
        length |= (packetLengthByteBuffer.get() & 0xff) << 8;
        length |= (packetLengthByteBuffer.get() & 0xff) << 16;
        length |= (packetLengthByteBuffer.get() & 0xff) << 20;
        return length - 4;
    }

    @Override
    public void handle(NetBuffer buffer) {
        if (!buffer.isOnlyOnePacket()) {
            DbException.throwInternalError("NetBuffer must be OnlyOnePacket");
        }
        try {
            int length = buffer.length();
            byte[] packet = new byte[length];
            buffer.read(packet, 0, length);
            buffer.recycle();
            ByteBufferBsonInput input = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(packet)));
            int requestID = input.readInt32();
            int responseTo = input.readInt32();
            int opCode = input.readInt32();
            logger.info("opCode: {}", opCode);
            switch (opCode) {
            case 2013: {
                handleMessage(input, requestID, responseTo);
                break;
            }
            case 2004: {
                handleQuery(input, requestID, responseTo, opCode);
                break;
            }
            default:
            }
        } catch (Throwable e) {
            logger.error("Failed to handle packet", e);
            sendErrorMessage(e);
        }
    }

    private void handleMessage(ByteBufferBsonInput input, int requestID, int responseTo) {
        input.readInt32();
        int type = input.readByte();
        BsonDocument response = null;
        switch (type) {
        case 0: {
            response = handleCommand(input);
            break;
        }
        case 1: {
            break;
        }
        default:
        }
        input.close();
        sendResponse(requestID, response);
    }

    private Database getDatabase(BsonDocument doc) {
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

    private Table getTable(BsonDocument doc, String key) {
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

    private ServerSession createSession(Database db) {
        return db.createSession(getUser(db));
    }

    private User getUser(Database db) {
        for (User user : db.getAllUsers()) {
            if (user.isAdmin())
                return user;
        }
        return db.getAllUsers().get(0);
    }

    private BsonDocument handleCommand(ByteBufferBsonInput input) {
        BsonDocument doc = decode(input);
        logger.info(doc.toJson());
        String command = doc.getFirstKey().toLowerCase();
        switch (command) {
        case "insert": {
            int n = 0;
            Table table = getTable(doc, "insert");
            try (ServerSession session = createSession(table.getDatabase())) {
                BsonArray documents = doc.getArray("documents", null);
                if (documents != null) {
                    for (int i = 0, size = documents.size(); i < size; i++) {
                        String json = documents.get(i).asDocument().toJson();
                        Row row = table.getTemplateRow();
                        row.setValue(0, ValueString.get(json));
                        table.addRow(session, row);
                        n++;
                    }
                }
                while (input.hasRemaining()) {
                    input.readByte();
                    input.readInt32(); // size
                    input.readCString();
                    doc = decode(input);
                    logger.info(doc.toJson());

                    String json = doc.toJson();
                    Row row = table.getTemplateRow();
                    row.setValue(0, ValueString.get(json));
                    table.addRow(session, row);
                    n++;
                }
                session.commit();
            }
            BsonDocument document = new BsonDocument();
            setOk(document);
            setN(document, n);
            return document;
        }
        case "find": {
            Table table = getTable(doc, "find");
            BsonArray documents = new BsonArray();
            try (ServerSession session = createSession(table.getDatabase())) {
                Cursor cursor = table.getScanIndex(session).find(session, null, null);
                while (cursor.next()) {
                    String json = cursor.get().getValue(0).getString();
                    // documents.add(new BsonString(json));
                    documents.add(BsonDocument.parse(json));
                }
                session.commit();
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
        case "delete": {
            int n = 0;
            Table table = getTable(doc, "delete");
            try (ServerSession session = createSession(table.getDatabase())) {
                Cursor cursor = table.getScanIndex(session).find(session, null, null);
                while (cursor.next()) {
                    table.removeRow(session, cursor.get());
                    n++;
                }
                session.commit();
            }
            BsonDocument document = new BsonDocument();
            setOk(document);
            setN(document, n);
            return document;
        }
        case "hello":
        case "ismaster": {
            BsonDocument document = new BsonDocument();
            document.append("ismaster", new BsonBoolean(true));
            document.append("connectionId", new BsonInt32(connectionId));
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
        default:
            BsonDocument document = new BsonDocument();
            setWireVersion(document);
            setOk(document);
            setN(document, 0);
            return document;
        }
    }

    private void setOk(BsonDocument doc) {
        doc.append("ok", new BsonInt32(1));
    }

    private void setN(BsonDocument doc, int n) {
        doc.append("n", new BsonInt32(n));
    }

    private void setWireVersion(BsonDocument doc) {
        doc.append("minWireVersion", new BsonInt32(0));
        doc.append("maxWireVersion", new BsonInt32(17));
    }

    private void handleQuery(ByteBufferBsonInput input, int requestID, int responseTo, int opCode) {
        input.readInt32();
        input.readCString();
        input.readInt32();
        input.readInt32();
        BsonDocument doc = decode(input);
        logger.info(doc.toJson());
        while (input.hasRemaining()) {
            BsonDocument returnFieldsSelector = decode(input);
            logger.info(returnFieldsSelector);
        }
        input.close();
        sendResponse(requestID);
    }

    private void sendResponse(int requestID) {
        BsonDocument document = new BsonDocument();
        document.append("minWireVersion", new BsonInt32(2));
        document.append("maxWireVersion", new BsonInt32(6));
        setOk(document);
        setN(document, 1);
        sendResponse(requestID, document);
    }

    private void sendResponse(int requestID, BsonDocument document) {
        BasicOutputBuffer out = new BasicOutputBuffer();
        out.writeInt32(0);
        out.writeInt32(requestID);
        out.writeInt32(requestID);
        out.writeInt32(1);

        out.writeInt32(0);
        out.writeInt64(0);
        out.writeInt32(0);
        out.writeInt32(1);

        Codec<BsonDocument> encoder = getCodec(document);
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(out);
        encoder.encode(bsonBinaryWriter, document, EncoderContext.builder().build());

        out.writeInt32(0, out.getPosition());
        sendMessage(out.toByteArray());
        out.close();
    }

    private BsonDocument decode(ByteBufferBsonInput input) {
        DecoderContext decoderContext = DecoderContext.builder().build();
        BsonBinaryReader reader = new BsonBinaryReader(input);
        List<BsonElement> keyValuePairs = new ArrayList<BsonElement>();
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            keyValuePairs.add(new BsonElement(fieldName, readValue(reader, decoderContext)));
        }
        reader.readEndDocument();
        return new BsonDocument(keyValuePairs);
    }

    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(
            asList(new ValueCodecProvider(), new BsonValueCodecProvider()));

    private BsonValue readValue(final BsonReader reader, final DecoderContext decoderContext) {
        return DEFAULT_REGISTRY
                .get(BsonValueCodecProvider.getClassForBsonType(reader.getCurrentBsonType()))
                .decode(reader, decoderContext);
    }

    @SuppressWarnings("unchecked")
    private Codec<BsonDocument> getCodec(final BsonDocument document) {
        return (Codec<BsonDocument>) DEFAULT_REGISTRY.get(document.getClass());
    }
}
