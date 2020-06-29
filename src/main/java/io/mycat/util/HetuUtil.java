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

package io.mycat.util;

import io.mycat.net.mysql.FieldPacket;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HetuUtil {

    private static final Pattern typePattern = Pattern.compile("^(.+?)\\((.+)\\)$");

    private static final int VARBINARY_MAX = 1024 * 1024 * 1024;

    // default is char(1)
    private static final int CHAR_DEFAULT = 1;

    // According to SQLGetTypeInfo
    private static final int VARCHAR_DEFAULT = 2048;

    // decimal max length
    private static final int DECIMAL_DEFAULT = 38;

    private static final int TIME_ZONE_MAX = 40;

    private static final int TIME_MAX = "HH:mm:ss.SSS".length();

    private static final int TIME_WITH_TIME_ZONE_MAX = TIME_MAX + TIME_ZONE_MAX;

    private static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();

    private static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;

    private static final int DATE_MAX = "yyyy-MM-dd".length();

    public static void serverTypeConvert(String hetuTypeInfo, FieldPacket pkg) {

        Matcher sepMatcher = typePattern.matcher(hetuTypeInfo);
        if (sepMatcher.find()) {
            String dataType = sepMatcher.group(1);
            String arguments = sepMatcher.group(2);
            serverTypeLength(dataType, pkg);
            pkg.type = (serverTypeMysqlStr(dataType) & 0xff);
            if (dataType.equalsIgnoreCase("decimal")) {
                pkg.length = Long.parseLong(arguments.split(",")[0] + 2);
                pkg.decimals = Byte.parseByte(arguments.split(",")[1]);
            } else if (dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar")) {
                pkg.length = Long.parseLong(arguments);
            }
        } else {
            pkg.type = (serverTypeMysqlStr(hetuTypeInfo) & 0xff);
            serverTypeLength(hetuTypeInfo, pkg);
        }
        if (MysqlDefs.isBianry((byte) pkg.type)) {
            // 63 represent binary character set
            pkg.charsetIndex = 63;
        }
    }

    public static void serverTypeLength(String hetuType, FieldPacket pkg) {
        switch (hetuType.toLowerCase()) {
            case "boolean":
                pkg.length = 1;
                break;
            case "bigint":
                pkg.length = 20;
                break;
            case "integer":
                pkg.length = 11;
                break;
            case "smallint":
                pkg.length = 6;
                break;
            case "tinyint":
                pkg.length = 4;
                break;
            case "real":
                pkg.length = 16;
                break;
            case "double":
                pkg.length = 24;
                break;
            case "char":
                pkg.length = CHAR_DEFAULT;
                break;
            case "varchar":
                pkg.length = VARCHAR_DEFAULT;
                break;
            case "varbinary":
                pkg.length = VARBINARY_MAX;
                break;
            case "time":
                pkg.length = TIME_MAX;
                break;
            case "time with time zone":
                pkg.length = TIME_WITH_TIME_ZONE_MAX;
                break;
            case "timestamp":
                pkg.length = TIMESTAMP_MAX;
                break;
            case "timestamp with time zone":
                pkg.length = TIMESTAMP_WITH_TIME_ZONE_MAX;
                break;
            case "date":
                pkg.length = DATE_MAX;
                break;
            case "interval year to month":
                pkg.length = TIMESTAMP_MAX;
                break;
            case "interval day to second":
                pkg.length = TIMESTAMP_MAX;
                break;
            case "decimal":
                pkg.length = DECIMAL_DEFAULT;
                break;
            default:
                pkg.length = 0;
                break;
        }
    }


    public static int serverTypeMysqlStr(String serverType) {
            if (serverType.equalsIgnoreCase("DECIMAL"))
                return MysqlDefs.FIELD_TYPE_NEW_DECIMAL;

            if (serverType.equalsIgnoreCase("TINYINT"))
                return MysqlDefs.FIELD_TYPE_TINY;

            if (serverType.equalsIgnoreCase("SMALLINT"))
                return MysqlDefs.FIELD_TYPE_SHORT;

            if (serverType.equalsIgnoreCase("INTEGER"))
                return MysqlDefs.FIELD_TYPE_LONG;

            if (serverType.equalsIgnoreCase("REAL"))
                return MysqlDefs.FIELD_TYPE_FLOAT;

            if (serverType.equalsIgnoreCase("DOUBLE"))
                return MysqlDefs.FIELD_TYPE_DOUBLE;

            if (serverType.equalsIgnoreCase("NULL"))
                return MysqlDefs.FIELD_TYPE_NULL;

            if (serverType.equalsIgnoreCase("TIMESTAMP"))
                return MysqlDefs.FIELD_TYPE_TIMESTAMP;

            if (serverType.equalsIgnoreCase("BIGINT"))
                return MysqlDefs.FIELD_TYPE_LONGLONG;

            if (serverType.equalsIgnoreCase("DATE"))
                return MysqlDefs.FIELD_TYPE_DATE;

            if (serverType.equalsIgnoreCase("TIME"))
                return MysqlDefs.FIELD_TYPE_TIME;

            if (serverType.equalsIgnoreCase("VARBINARY"))
                return MysqlDefs.FIELD_TYPE_TINY_BLOB;

            if (serverType.equalsIgnoreCase("VARCHAR"))
                return MysqlDefs.FIELD_TYPE_VAR_STRING;

            if (serverType.equalsIgnoreCase("CHAR"))
                return MysqlDefs.FIELD_TYPE_STRING;

            if (serverType.equalsIgnoreCase("BOOLEAN"))
                return MysqlDefs.FIELD_TYPE_BIT;

            // 匹配返回的unknown类型
            if (serverType.equalsIgnoreCase("UNKNOWN"))
                return MysqlDefs.FIELD_TYPE_UNKNOWN;

            else
                //for interval, map, arrray, json, row etc
                return MysqlDefs.FIELD_TYPE_VAR_STRING;   // 其他未知类型返回字符类型
            // return Types.VARCHAR;
        }

    public static void resultSetToFieldPacket(String charset,
        List<FieldPacket> fieldPks, ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int colunmCount = metaData.getColumnCount();
        if (colunmCount > 0) {
            for (int i = 0; i < colunmCount; i++) {
                int j = i + 1;
                FieldPacket fieldPacket = new FieldPacket();
                fieldPacket.catalog = StringUtil.encode(metaData.getCatalogName(j), charset);
                fieldPacket.db = StringUtil.encode(metaData.getSchemaName(j), charset);
                fieldPacket.table = StringUtil.encode(metaData.getTableName(j), charset);
                fieldPacket.orgTable = StringUtil.encode(metaData.getTableName(j), charset);
                fieldPacket.name = StringUtil.encode(metaData.getColumnLabel(j), charset);
                fieldPacket.orgName = StringUtil.encode(metaData.getColumnName(j), charset);
                fieldPacket.flags = ResultSetUtil.toFlag(metaData, j);

                fieldPacket.length = metaData.getColumnDisplaySize(j);

                // for more detail about mysql packet-Protocol, pls see web:
                // https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition
                int jdbcColumnType = metaData.getColumnType(j);
                // boolean will map to bit type for mysql C/S not support boolean
                if (Types.BOOLEAN == jdbcColumnType) {
                    fieldPacket.type = (byte) (MysqlDefs.FIELD_TYPE_BIT & 0xff);
                    fieldPacket.length = 1;
                }
                //check that jdbc driver not implement with 4.2 Specification vesion(new type for TIME_WITH_TIMEZONE)
                //identify time and time with timezone by getColumnTypeName
                else  if(Types.TIME == metaData.getColumnType(j)
                        & "time with time zone".equalsIgnoreCase(metaData.getColumnTypeName(j)))
                {
                    fieldPacket.type = (byte) (MysqlDefs.FIELD_TYPE_VAR_STRING & 0xff);
                }
                //check that jdbc driver not implement with 4.2 Specification vesion(new type for TIMESTAMP_WITH_TIMEZONE)
                //identify timestamp and timestamp with timezone by getColumnTypeName
                else  if(Types.TIMESTAMP == metaData.getColumnType(j)
                        & "timestamp with time zone".equalsIgnoreCase(metaData.getColumnTypeName(j)))
                {
                    fieldPacket.type = (byte) (MysqlDefs.FIELD_TYPE_VAR_STRING & 0xff);
                }
                // data type: interval, array, map, row, JSON are not standard JDBC type, customize them
                else if (Types.JAVA_OBJECT == jdbcColumnType) {
                    // 0x1f for dynamic strings, double, float
                    fieldPacket.decimals = (byte) 0x1f;
                    fieldPacket.type = (byte) (MysqlDefs.FIELD_TYPE_VAR_STRING & 0xff);
                } else {
                    if (Types.TINYINT == jdbcColumnType || Types.SMALLINT == jdbcColumnType
                        || Types.INTEGER == jdbcColumnType || Types.BIGINT == jdbcColumnType
                        || Types.NCHAR == jdbcColumnType || Types.CHAR == jdbcColumnType) {
                        // 0x00 for integers and static strings
                        fieldPacket.decimals = (byte) 0x00;
                    } else if (Types.VARCHAR == jdbcColumnType || Types.NVARCHAR == jdbcColumnType
                        || Types.CLOB == jdbcColumnType || Types.LONGNVARCHAR == jdbcColumnType
                        || Types.NCLOB == jdbcColumnType || Types.REAL == jdbcColumnType
                        || Types.DOUBLE == jdbcColumnType) {
                        // 0x1f for dynamic strings, double, float
                        fieldPacket.decimals = (byte) 0x1f;
                    } else if (Types.DECIMAL == jdbcColumnType) {
                        // 0x00 to 0x51 for decimals
                        // here we use decimal scale to set it
                        fieldPacket.decimals = (byte) metaData.getScale(j);
                    } else {
                        // 0x1f for dynamic strings, double, float
                        // other unknown type will cast to MysqlDefs.FIELD_TYPE_VAR_STRING, so set decimals to 0x1f
                        fieldPacket.decimals = (byte) 0x1f;
                    }
                    // map to mysql C/S
                    fieldPacket.type = (byte) (MysqlDefs.javaTypeMysql(jdbcColumnType) & 0xff);
                }

                if (MysqlDefs.isBianry((byte) fieldPacket.type)) {
                    fieldPacket.charsetIndex = 63;
                }
                fieldPks.add(fieldPacket);
            }
        }

    }
}
