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

import java.nio.ByteBuffer;

import io.mycat.net.mysql.OkPacket;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public final class UseHandler {

    public static void handle(String sql, ServerConnection c, int offset) {
        String rawschema = sql.substring(offset).trim();
        int length = rawschema.length();
        if (length > 0) {
        	//许多客户端工具断链重连后会批量发送SQL,如下:
            //USE `TESTDB`;\nSELECT SYSDATE(),CURRENT_USER()
            //modify by jeff.cao 2018/3/31
            int end = rawschema.indexOf(";");
            if (end > 0) {
                rawschema = rawschema.substring(0, end - 1);
            }
            rawschema = StringUtil.replaceChars(rawschema, "`", null);
            length = rawschema.length();
            if (rawschema.charAt(0) == '\'' && rawschema.charAt(length - 1) == '\'') {
                rawschema = rawschema.substring(1, length - 1);
            }
        }
        String[] catschem = rawschema.split("\\.");
        if(catschem.length>1) {
            c.backConn.setCatalog(catschem[0]);
            c.setCatalog(catschem[0]);
            c.backConn.setSchema(catschem[1]);
            c.setSchema(catschem[1]);
        } else {
            c.backConn.setSchema(catschem[0]);
            c.setSchema(catschem[0]);
        }

        ByteBuffer buffer = c.allocate();
        c.write(c.writeToBuffer(OkPacket.OK, buffer));
    }

}
