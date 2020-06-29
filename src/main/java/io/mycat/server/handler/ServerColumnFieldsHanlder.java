/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 */

package io.mycat.server.handler;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.backend.mysql.nio.handler.MiddlerResultHandler;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.backend.hetu.HetuConnection;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.net.handler.ColumnFieldsHandler;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// add ServerColumnFieldsHanlder
public class ServerColumnFieldsHanlder implements ResponseHandler, ColumnFieldsHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerColumnFieldsHanlder.class);

    private ServerConnection source;

    private volatile byte packetId;

    private byte[] data;

    private long netOutBytes;

    private int fieldCount;

    private volatile ByteBuffer buffer;

    private List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();

    private byte[] header = null;

    private List<byte[]> fields = null;

    public ServerColumnFieldsHanlder(ServerConnection source) {
        this.source = source;
    }

    @Override
    public void handle(byte[] data) {
        this.data = data;
        // String schemaName = "default";
        String schemaName = source.catalog;
        SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(schemaName);
        RouteResultsetNode node = new RouteResultsetNode(schema.getDataNode(), ServerParse.SELECT, "Field_list");
        MycatConfig conf = MycatServer.getInstance().getConfig();

        PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
        try {
            dn.getConnection(dn.getDatabase(), source.isAutocommit(), node, this, node);
        } catch (Exception e) {
            LOGGER.warn(new StringBuilder().append(source).toString(), e);
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {

    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        String table;
        try {
            MySQLMessage mm = new MySQLMessage(data);
            mm.position(5);
            table = mm.readString("utf-8");
        } catch (UnsupportedEncodingException e) {
            LOGGER.warn("Unknown charset");
            return;
        }
        conn.setResponseHandler(this);
        if (conn instanceof HetuConnection) {
            ((HetuConnection) conn).fieldList(source, table);
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        errPacket.packetId = ++packetId;
        source.writeErrMessage(errPacket.errno, new String(errPacket.message));
        LOGGER.error("Fieldlist command raises error" + errPacket.message);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {

    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
        this.header = header;
        this.fields = fields;
        MiddlerResultHandler middlerResultHandler = source.getSession2().getMiddlerResultHandler();
        if (null != middlerResultHandler) {
            return;
        }
        // this.netOutBytes += header.length;
        for (int i = 0, len = fields.size(); i < len; ++i) {
            byte[] field = fields.get(i);
            this.netOutBytes += field.length;
        }

        // header[3] = ++packetId;
        // buffer = source.writeToBuffer(header, allocBuffer());
        buffer = null;
        for (int i = 0, len = fields.size(); i < len; ++i) {
            byte[] field = fields.get(i);
            field[3] = ++packetId;

            // 保存field信息
            FieldPacket fieldPk = new FieldPacket();
            fieldPk.read(field);
            fieldPackets.add(fieldPk);

            if (buffer == null) {
                buffer = source.writeToBuffer(field, allocBuffer());
            } else {
                buffer = source.writeToBuffer(field, buffer);
            }
        }

        fieldCount = fieldPackets.size();

        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, buffer);

        if (source.canResponse()) {
            source.write(buffer);
        }
    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {

    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {

    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {

    }

    public void _execute(final BackendConnection conn, String table) {

    }

    private ByteBuffer allocBuffer() {
        if (buffer == null) {
            buffer = source.allocate();
        }
        return buffer;
    }
}
