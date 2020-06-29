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

package io.mycat.backend.hetu;

import io.mycat.backend.hetu.odbc.escapesequences.QueryParserUtil;
import io.mycat.backend.jdbc.JDBCConnection;
import io.mycat.backend.mysql.PreparedStatement;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.HetuUtil;
import io.mycat.util.StringUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

// based on JDBC connection
public class HetuConnection
        extends JDBCConnection {
    protected static final Logger LOGGER = LoggerFactory.getLogger(HetuConnection.class);

    private volatile String catalog;

    public HetuConnection() {
        super();
    }

    @Override
    public void close(String reason) {
        try {
            con.close();
            if (processor != null) {
                processor.removeConnection(this);
            }

        } catch (SQLException e) {
        }
    }

    public String getCatalog() {
        return this.catalog;
    }

    public void setCatalog(String newCatalog) {
        this.catalog = newCatalog;
    }

    @Override
    protected void executeSQL(RouteResultsetNode rrn, ServerConnection sc, boolean autocommit) throws IOException {
        String orgin = rrn.getStatement();
        // String sql = rrn.getStatement().toLowerCase();
        if (!modifiedSQLExecuted && rrn.isModifySQL()) {
            modifiedSQLExecuted = true;
        }

        // replace \\
        // orgin = orgin.replace("\\\\", "");
        orgin = orgin.replace('\0', ' ');
        // handle the ODBC Escape Sequences
        orgin = QueryParserUtil.parseAndRewriteQuery(orgin);
        try {
            // set connection schema
            con.setSchema(sc.getSchema());

            syncIsolation(sc.getTxIsolation());

            con.setCatalog(sc.getCatalog());
//            con.setSchema(schema);

            int sqlType = rrn.getSqlType();
            if (rrn.isCallStatement() && "oracle".equalsIgnoreCase(getDbType())) {
                // 存储过程暂时只支持oracle
                ouputCallStatement(rrn, sc, orgin);
            } else if (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW) {
                // WITH HARD CODE
                // orgin = orgin.replace('`', '\"');
                ouputResultSet(sc, orgin);
            } else {
                executeddl(sc, orgin);
            }

        } catch (SQLException e) {
            String msg = e.getMessage();
            LOGGER.error("Direct execute command raises error: " + "\n"
                    + "Message is: " + msg + "\n"
                    + "Cause is: " + e.getCause().getMessage());
            ErrorPacket error = new ErrorPacket();
            error.packetId = ++packetId;
            error.errno = e.getErrorCode();
            error.message = msg.getBytes();
            this.respHandler.errorResponse(error.writeToBytes(sc), this);
        } catch (Exception e) {
            String msg = e.getMessage();
            ErrorPacket error = new ErrorPacket();
            error.packetId = ++packetId;
            error.errno = ErrorCode.ER_UNKNOWN_ERROR;
            error.message = ((msg == null) ? e.toString().getBytes() : msg.getBytes());
            String err = null;
            if (error.message != null) {
                err = new String(error.message);
            }
            LOGGER.error("sql execute error: " + err, e);
            this.respHandler.errorResponse(error.writeToBytes(sc), this);
        } finally {
            this.running = false;
        }

    }

    @Override
    protected void executeddl(ServerConnection sc, String sql) throws SQLException {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            int count = stmt.executeUpdate(sql);
            OkPacket okPck = new OkPacket();
            okPck.affectedRows = count;
            okPck.insertId = 0;
            okPck.packetId = ++packetId;
            okPck.message = " OK!".getBytes();
            this.respHandler.okResponse(okPck.writeToBytes(sc), this);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {

                }
            }
        }
    }

    public void fieldList(ServerConnection sc, String sql) {
        {
            ResultSet rs = null;
            Statement stmt = null;
            sql = sql.replace('\0', ' ');

            sql = "select * from " + sc.getSchema() + '.' + sql + " limit 1";

            try {
                // set connection schema
                con.setCatalog(catalog);
                con.setSchema(schema);

                stmt = con.createStatement();
                rs = stmt.executeQuery(sql);

                List<FieldPacket> fieldPks = new LinkedList<FieldPacket>();
                HetuUtil.resultSetToFieldPacket(sc.getCharset(), fieldPks, rs);
                int colunmCount = fieldPks.size();
                ByteBuffer byteBuf = sc.allocate();
                ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
                headerPkg.fieldCount = fieldPks.size();
                headerPkg.packetId = ++packetId;

                byteBuf = headerPkg.write(byteBuf, sc, true);
                byteBuf.flip();
                byte[] header = new byte[byteBuf.limit()];
                byteBuf.get(header);
                byteBuf.clear();
                List<byte[]> fields = new ArrayList<byte[]>(fieldPks.size());
                Iterator<FieldPacket> itor = fieldPks.iterator();
                while (itor.hasNext()) {
                    FieldPacket curField = itor.next();
                    curField.packetId = ++packetId;
                    byteBuf = curField.write(byteBuf, sc, false);
                    byteBuf.flip();
                    byte[] field = new byte[byteBuf.limit()];
                    byteBuf.get(field);
                    byteBuf.clear();
                    fields.add(field);
                }
                EOFPacket eofPckg = new EOFPacket();
                eofPckg.packetId = ++packetId;
                byteBuf = eofPckg.write(byteBuf, sc, false);
                byteBuf.flip();
                byte[] eof = new byte[byteBuf.limit()];
                byteBuf.get(eof);
                byteBuf.clear();
                this.respHandler.fieldEofResponse(header, fields, eof, this);

                fieldPks.clear();
            } catch (SQLException e) {
                String msg = e.getMessage();
                LOGGER.error("COM_FIELD_LIST command raises error: " + "\n"
                    + "Message is: " + msg + "\n"
                    + "Cause is: " + e.getCause().getMessage());
                ErrorPacket error = new ErrorPacket();
                error.packetId = ++packetId;
                error.errno = e.getErrorCode();
                error.message = msg.getBytes();
                this.respHandler.errorResponse(error.writeToBytes(sc), this);
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException e) {

                    }
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {

                    }
                }
            }
        }
    }

    public List<String> prepareParameters(ServerConnection sc, String sql, long prepareId, PreparedStatement pstmt) {
        ResultSet rsInput = null;
        ResultSet rsOutput = null;

        Statement stmt = null;
        Statement stmtInput = null;
        Statement stmtOutput = null;
        Statement stmtDealloc = null;

        String executeSql = "";
        List<String> parameters = new ArrayList<>();

        int colNum = 0;
        try {
            con.setCatalog(catalog);
            con.setSchema(schema);

            executeSql = "PREPARE STMT" + prepareId + " FROM " + sql;
            stmt = con.createStatement();
            stmt.execute(executeSql);

            executeSql = "DESCRIBE INPUT " + "STMT" + prepareId;
            stmtInput = con.createStatement();
            rsInput = stmtInput.executeQuery(executeSql);
            // the result column of describe input
            // Position | Type
            while (rsInput.next()) {
                parameters.add(rsInput.getString(2));
            }

            executeSql = "DESCRIBE OUTPUT " + "STMT" + prepareId;
            stmtOutput = con.createStatement();
            rsOutput = stmtOutput.executeQuery(executeSql);
            String charset = sc.getCharset();
            List<FieldPacket> fieldPks = new LinkedList<>();
            while (rsOutput.next()) {
                FieldPacket fieldPacket = new FieldPacket();
                // the resultset's column info of describe out command:
                // Column Name | Catalog | Schema  |    Table    |  Type   | Type Size | Aliased
                fieldPacket.orgName = StringUtil.encode(rsOutput.getString(1), charset);
                fieldPacket.name = StringUtil.encode(rsOutput.getString(1), charset);
                fieldPacket.orgTable = StringUtil.encode(rsOutput.getString(4), charset);
                fieldPacket.table = StringUtil.encode(rsOutput.getString(4), charset);
                fieldPacket.db = StringUtil.encode(rsOutput.getString(3), charset);
                String type = rsOutput.getString(5);
                HetuUtil.serverTypeConvert(type, fieldPacket);
                fieldPks.add(fieldPacket);
                colNum++;
            }
            pstmt.setOutFieldPks(fieldPks);
            pstmt.setColumnsNumber(colNum);

        } catch (SQLException e) {
            String msg = e.getMessage();
            LOGGER.error("prepareParameters command raises error: " + "\n"
                    + "Message is: " + msg + "\n"
                    + "catalog is:" + catalog + ", schema is:" + schema + "\n"
                    + "Cause is: " + e.getCause().getMessage());
            ErrorPacket error = new ErrorPacket();
            error.packetId = ++packetId;
            error.errno = e.getErrorCode();
            error.message = msg.getBytes();
            this.respHandler.errorResponse(error.writeToBytes(sc), this);
            return null;
        } finally {
            executeSql = "DEALLOCATE PREPARE " + "STMT" + prepareId;
            try {
                stmtDealloc = con.createStatement();
                stmtDealloc.execute(executeSql);
            } catch (SQLException e) {
                LOGGER.error("DEALLOCATE PREPARE STMT raise error!");
            }
            if (rsInput != null) {
                try {
                    rsInput.close();
                } catch (SQLException e) {

                }
            }
            if (rsOutput != null) {
                try {
                    rsOutput.close();
                } catch (SQLException e) {

                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {

                }
            }
            if (stmtInput != null) {
                try {
                    stmtInput.close();
                } catch (SQLException e) {

                }
            }
            if (stmtOutput != null) {
                try {
                    stmtOutput.close();
                } catch (SQLException e) {

                }
            }
            if (stmtDealloc != null) {
                try {
                    stmtDealloc.close();
                } catch (SQLException e) {

                }
            }
        }

        return parameters;
    }

    public String getServerVersion() {
        try {
            DatabaseMetaData metaData = con.getMetaData();
            return metaData.getDatabaseProductVersion();
        }catch (SQLException e) {
            LOGGER.error("Get server version error:", e);
            return new String("Invalid version");
        }
    }

}
