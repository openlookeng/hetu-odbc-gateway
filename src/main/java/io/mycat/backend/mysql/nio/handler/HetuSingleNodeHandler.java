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

package io.mycat.backend.mysql.nio.handler;

import io.mycat.backend.BackendConnection;
import io.mycat.route.RouteResultset;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HetuSingleNodeHandler
        extends SingleNodeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HetuSingleNodeHandler.class);

    public HetuSingleNodeHandler(RouteResultset rrs, NonBlockingSession session) {
        this.rrs = rrs;
        this.node = rrs.getNodes()[0];

        if (node == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }

        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }

        this.session = session;
        ServerConnection source = session.getSource();
        String schema = source.getSchema();

        schema = source.catalog;

        if (rrs != null && rrs.getStatement() != null) {
            netInBytes += rrs.getStatement().getBytes().length;
        }
    }

    @Override
    public void execute() throws Exception {
        startTime = System.currentTimeMillis();
        ServerConnection sc = session.getSource();
        this.isRunning = true;
        this.packetId = 0;
        final BackendConnection conn = sc.backConn;

        try {
            _execute(conn);
        } catch (Exception e) {
            ServerConnection source = session.getSource();
            LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
            // 设置错误
            connectionError(e, null);
        }
    }
}
