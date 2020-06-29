/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */

package io.mycat.server.handler;

import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.google.common.escape.Escapers.Builder;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.hetu.HetuConnection;
import io.mycat.backend.mysql.BindValue;
import io.mycat.backend.mysql.ByteUtil;
import io.mycat.backend.mysql.PreparedStatement;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.net.handler.FrontendPrepareHandler;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.ExecutePacket;
import io.mycat.net.mysql.LongDataPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.ResetPacket;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.response.PreparedStmtResponse;
import io.mycat.util.HexFormatUtil;
import io.mycat.util.HetuUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat, CrazyPig, zhuam
 */
public class ServerPrepareHandler implements FrontendPrepareHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerPrepareHandler.class);

  private static Escaper varcharEscaper = null;

  static {
    Builder escapeBuilder = Escapers.builder();
    escapeBuilder.addEscape('\'', "\\'");
    escapeBuilder.addEscape('\"', "\\\"");
    escapeBuilder.addEscape('$', "\\$");
    varcharEscaper = escapeBuilder.build();
  }

    private ServerConnection source;

    private static final AtomicLong PSTMT_ID_GENERATOR = new AtomicLong(0);

    private final Map<Long, PreparedStatement> pstmtForId = new ConcurrentHashMap<>();

    private int maxPreparedStmtCount;

    private Map<Long, int[]> parameterForId = new ConcurrentHashMap<>();

    private volatile byte packetId;

    public ServerPrepareHandler(ServerConnection source, int maxPreparedStmtCount) {
        this.source = source;
        this.maxPreparedStmtCount = maxPreparedStmtCount;
    }

    @Override
    public void prepare(String sql) {
        // 解析获取字段个数和参数个数
        int columnCount = 0;
        int paramCount = getParamCount(sql);
        if (paramCount > maxPreparedStmtCount) {
            source.writeErrMessage(ErrorCode.ER_PS_MANY_PARAM, "Prepared statement contains too many placeholders");
            return;
        }
        PreparedStatement pstmt =
                new PreparedStatement(PSTMT_ID_GENERATOR.incrementAndGet(), sql, columnCount, paramCount);
        pstmtForId.put(pstmt.getId(), pstmt);
        LOGGER.debug("preparestatement prepare id:{}", pstmt.getId());

        initPrepareHandler prepare = new initPrepareHandler(source, sql, pstmt.getId());
        prepare.initBackendConnection();

        int sqlType = ServerParse.parse(sql);
        if (sqlType == ServerParse.INSERT || sqlType == ServerParse.UPDATE || sqlType == ServerParse.DELETE) {
            pstmt.setColumnsNumber(0);
        }

        int[] ParameterTypes = parameterForId.get(pstmt.getId());
        if (ParameterTypes != null) {
            PreparedStmtResponse.response(pstmt, source, ParameterTypes);
        }
    }

    @Override
    public void sendLongData(byte[] data) {
        LongDataPacket packet = new LongDataPacket();
        packet.read(data);
        long pstmtId = packet.getPstmtId();
        PreparedStatement pstmt = pstmtForId.get(pstmtId);
        if (pstmt != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("send long data to prepare sql : " + pstmtForId.get(pstmtId));
            }
            long paramId = packet.getParamId();
            try {
                pstmt.appendLongData(paramId, packet.getLongData());
            } catch (IOException e) {
                source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPTION, e.getMessage());
            }
        }
    }

    @Override
    public void reset(byte[] data) {
        ResetPacket packet = new ResetPacket();
        packet.read(data);
        long pstmtId = packet.getPstmtId();
        PreparedStatement pstmt = pstmtForId.get(pstmtId);
        if (pstmt != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("reset prepare sql : " + pstmtForId.get(pstmtId));
            }
            pstmt.resetLongData();
            source.write(OkPacket.OK);
        } else {
            source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPTION,
                    "can not reset prepare statement : " + pstmtForId.get(pstmtId));
        }
    }

    @Override
    public void execute(byte[] data) {
        long pstmtId = ByteUtil.readUB4(data, 5);
        PreparedStatement pstmt = null;
        LOGGER.debug("preparestatement execute id:{}", pstmtId);
        if ((pstmt = pstmtForId.get(pstmtId)) == null) {
            LOGGER.error("Unknown pstmtId when executing: " + pstmtId);
            source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown pstmtId when executing.");
        } else {
            // for (int i = 0; i < pstmt.getParametersNumber(); i++) {
            // pstmt.getParametersType()[i] = ParameterTypes[i];
            // }

            ExecutePacket packet = new ExecutePacket(pstmt);
            try {
                packet.read(data, source.getCharset());
            } catch (UnsupportedEncodingException e) {
                source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, e.getMessage());
                return;
            }
            BindValue[] bindValues = packet.values;
            // 还原sql中的动态参数为实际参数值
            String sql = prepareStmtBindValue(pstmt, bindValues);
            // 执行sql
            source.getSession2().setPrepared(true);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute prepare sql: " + sql);
            }
            source.query(sql);
            pstmt.resetLongData();
        }
    }

    @Override
    public void close(byte[] data) {
        long pstmtId = ByteUtil.readUB4(data, 5); // 获取prepare stmt id
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("close prepare stmt, stmtId = " + pstmtId);
        }
        // PreparedStatement pstmt = pstmtForId.remove(pstmtId);
        // parameterForId.remove(pstmtId);
        clear();
    }

    @Override
    public void clear() {
        this.pstmtForId.clear();
        this.parameterForId.clear();
    }

    // TODO 获取预处理语句中column的个数
    private int getColumnCount(String sql) {
        int columnCount = 0;
        // TODO ...
        return columnCount;
    }

    // 获取预处理sql中预处理参数个数
    private int getParamCount(String sql) {
        char[] cArr = sql.toCharArray();
        int count = 0;
        for (int i = 0; i < cArr.length; i++) {
            if (cArr[i] == '?') {
                count++;
            }
        }
        return count;
    }

    /**
     * 组装sql语句,替换动态参数为实际参数值
     *
     * @param pstmt
     * @param bindValues
     * @return
     */
    private String prepareStmtBindValue(PreparedStatement pstmt, BindValue[] bindValues) {
        String sql = pstmt.getStatement();
        int[] paramTypes = pstmt.getParametersType();

        StringBuilder sb = new StringBuilder();
        int idx = 0;
        for (int i = 0, len = sql.length(); i < len; i++) {
            char c = sql.charAt(i);
            if (c != '?') {
                sb.append(c);
                continue;
            }
            // 处理占位符?
            // int paramType = paramTypes[idx];
            // using the paramter types that we stored before
            int paramType = parameterForId.get(pstmt.getId())[idx];
            if (paramType < 0) {
                paramType = paramTypes[idx];
            }
            BindValue bindValue = bindValues[idx];
            idx++;
            // 处理字段为空的情况
            if (bindValue.isNull) {
                sb.append("NULL");
                continue;
            }
            // 非空情况, 根据字段类型获取值
            switch (paramType & 0xff) {
                case Fields.FIELD_TYPE_TINY:
                    // sb.append(String.valueOf(bindValue.byteBinding));
                    // break;
                case Fields.FIELD_TYPE_SHORT:
                    // sb.append(String.valueOf(bindValue.shortBinding));
                    // break;
                case Fields.FIELD_TYPE_LONG:
                    // sb.append(String.valueOf(bindValue.intBinding));
                    // break;
                case Fields.FIELD_TYPE_LONGLONG:
                    // sb.append(String.valueOf(bindValue.longBinding));
                    // break;
                case Fields.FIELD_TYPE_FLOAT:
                    // sb.append(String.valueOf(bindValue.floatBinding));
                    // break;
                case Fields.FIELD_TYPE_BIT:
                    // sb.append(String.valueOf(bindValue.floatBinding));
                    // break;
                case Fields.FIELD_TYPE_DOUBLE:
                    // sb.append(String.valueOf(bindValue.doubleBinding));
                    sb.append(bindValue.value);
                    break;
                case Fields.FIELD_TYPE_VAR_STRING:
                case Fields.FIELD_TYPE_STRING:
                case Fields.FIELD_TYPE_VARCHAR:
                    bindValue.value = varcharEscaper.asFunction().apply(String.valueOf(bindValue.value));
                    sb.append("'" + bindValue.value + "'");
                    break;
                case Fields.FIELD_TYPE_TINY_BLOB:
                case Fields.FIELD_TYPE_BLOB:
                case Fields.FIELD_TYPE_MEDIUM_BLOB:
                case Fields.FIELD_TYPE_LONG_BLOB:
                    if (bindValue.value instanceof ByteArrayOutputStream) {
                        byte[] bytes = ((ByteArrayOutputStream) bindValue.value).toByteArray();
                        sb.append("X'" + HexFormatUtil.bytesToHexString(bytes) + "'");
                    } else {
                        // 正常情况下不会走到else, 除非long data的存储方式(ByteArrayOutputStream)被修改
                        LOGGER.warn(
                                "bind value is not a instance of ByteArrayOutputStream, maybe someone change the implement of long data storage!");
                        sb.append("'" + bindValue.value + "'");
                    }
                    break;
                case Fields.FIELD_TYPE_TIME:
                case Fields.FIELD_TYPE_DATE:
                case Fields.FIELD_TYPE_DATETIME:
                case Fields.FIELD_TYPE_TIMESTAMP:
                    sb.append("'" + bindValue.value + "'");
                    break;
                default:
                    bindValue.value = varcharEscaper.asFunction().apply(String.valueOf(bindValue.value));
                    sb.append(bindValue.value.toString());
                    break;
            }
        }
        return sb.toString();
    }

    public class initPrepareHandler implements ResponseHandler {
        private String tmpSql;

        private long pstmtId;

        private ServerConnection source;

        public initPrepareHandler(ServerConnection source, String tmpSql, long pstmtId) {
            this.source = source;
            this.tmpSql = tmpSql;
            this.pstmtId = pstmtId;
        }

        public void initBackendConnection() {
            connectionAcquired(source.backConn);
        }

        @Override
        public void connectionError(Throwable e, BackendConnection conn) {

        }

        @Override
        public void connectionAcquired(BackendConnection conn) {
            conn.setResponseHandler(this);
            List<String> parameterList = new ArrayList<>();
            PreparedStatement pstmt = pstmtForId.get(pstmtId);

            if (conn instanceof HetuConnection) {
                parameterList = ((HetuConnection) conn).prepareParameters(source, tmpSql, pstmtId, pstmt);
            }

            int[] ParameterTypes = null;
            if (parameterList != null) {
                ParameterTypes = new int[parameterList.size()];
                for (int i = 0; i < parameterList.size(); i++) {
                    ParameterTypes[i] = (byte) (HetuUtil.serverTypeMysqlStr(parameterList.get(i)) & 0xff);
                }
                parameterForId.put(pstmtId, ParameterTypes);
            }
        }

        @Override
        public void errorResponse(byte[] err, BackendConnection conn) {
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.read(err);
            errPacket.packetId = ++packetId;
            source.writeErrMessage(errPacket.errno, new String(errPacket.message));
        }

        @Override
        public void okResponse(byte[] ok, BackendConnection conn) {

        }

        @Override
        public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {

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
    }
}
