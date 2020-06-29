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

import io.mycat.backend.BackendConnection;
import io.mycat.config.model.DBHostConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class HetuConnectionFactory
{
    public BackendConnection createNewConnection(DBHostConfig cfg, Properties properties, String catalog, String schema) throws SQLException {
        HetuConnection c = new HetuConnection();
        c.setHost(cfg.getIp());
        c.setPort(cfg.getPort());
        c.setCatalog(catalog);
        c.setSchema(schema);
        c.setDbType(cfg.getDbType());
        Connection connection = DriverManager.getConnection(cfg.getUrl(), properties);
        connection.setCatalog(catalog);
        c.setCon(connection);
        return c;
    }
}
