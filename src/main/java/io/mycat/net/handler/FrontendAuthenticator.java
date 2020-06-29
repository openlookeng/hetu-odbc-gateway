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
package io.mycat.net.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Properties;

import io.mycat.backend.jdbc.JDBCConnection;
import io.mycat.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.SecurityUtil;
import io.mycat.backend.hetu.HetuConnectionFactory;
import io.mycat.config.Capabilities;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.net.FrontendConnection;
import io.mycat.net.NIOHandler;
import io.mycat.net.NIOProcessor;
import io.mycat.net.mysql.AuthPacket;
import io.mycat.net.mysql.MySQLPacket;
import io.mycat.net.mysql.QuitPacket;

/**
 * 前端认证处理器
 *
 * @author mycat
 */
public class FrontendAuthenticator implements NIOHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendAuthenticator.class);
    private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };

    protected final FrontendConnection source;

    public FrontendAuthenticator(FrontendConnection source) {
        this.source = source;
    }

    private HetuConnectionFactory connectionFactory = new HetuConnectionFactory();

    private final String serverSchema = "_server_schema";

    private final String serverConnectFile = "_server_connect_file";

    private final String serverConnectUrl = "_server_connect_url";

    private int serverDefaultPort = 8080;

    private final String stringUser = "user";

    private final String stringIp = "ip";

    private final String stringPort = "port";

    private final int maxMessageLen = 128;

    @Override
    public void handle(byte[] data) {
        // check quit packet
        if (data.length == QuitPacket.QUIT.length && data[4] == MySQLPacket.COM_QUIT) {
            source.close("quit packet");
            return;
        }

        AuthPacket auth = new AuthPacket();
        auth.read(data);

        String url = auth.connectAttrs.get(serverConnectUrl);

        try {
            String ip;
            int port = serverDefaultPort;
            Properties connectProperty = new Properties();//property to connect to backend server
            Properties hostInfo = new Properties();   //inner property

            if (url == null) {
                failure(ErrorCode.ER_SERVER_SHUTDOWN, "server url is null");
                return;
            }

            String propertyFile = auth.connectAttrs.get(serverConnectFile);
            if (propertyFile != null){
                readConnectProperty(propertyFile, connectProperty);
            }

            parseHostProperty(auth, connectProperty, hostInfo);
            SystemConfig sys = MycatServer.getInstance().getConfig().getSystem();
            String serverPrefix = sys.getJdbcUrlPrefix();
            url = serverPrefix + url;
            auth.user = hostInfo.getProperty(stringUser);
            ip = hostInfo.getProperty(stringIp);
            port = Integer.parseInt(hostInfo.getProperty(stringPort));

            DBHostConfig config = new DBHostConfig(auth.user, ip, port, url, auth.user, null, null);
            config.setDbType("Hetu");
            String schema = auth.connectAttrs.get(serverSchema);
            source.backConn = connectionFactory.createNewConnection(config, connectProperty, auth.database, schema);
            JDBCConnection backConn = (JDBCConnection)source.backConn;
            backConn.setFrontConn((ServerConnection) source);
        } catch (SQLException e) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied, error info: " + e.getMessage());
            return;
        }

        success(auth);

    }

    /**
     * 读取propertyFile里的property参数放入properties
     * @param propertyFile
     * @param properties
     */
    private void readConnectProperty(String propertyFile, Properties properties) {
        try (InputStream configIS = new FileInputStream(propertyFile)) {
            if (configIS == null) {
                LOGGER.error("Read file error. file : " + propertyFile);
                return;
            }

            properties.load(configIS);
        } catch (IOException e) {
            LOGGER.error("Load jdbcBackend propersites error:", e);
            throw new RuntimeException("Can't find jdbc backend config properties file : " + propertyFile);
        }
    }

    /**
     * 读取auth和connectProperty里的host信息放入properties
     * @param auth
     * @param connectProperty
     * @param properties
     */
    private void parseHostProperty(AuthPacket auth, Properties connectProperty, Properties properties) {
        String url = auth.connectAttrs.get(serverConnectUrl);
        if (url == null) {
            LOGGER.error("Presto url is null");
            return;
        }

        String ip;
        String port;
        int index = url.indexOf(':');
        int endIndex;

        if (index == -1) {
            ip = url.trim();
            port = new String();
            port = serverDefaultPort + "";
        } else {
            ip = url.substring(0, index).trim();

            for (endIndex = index + 1; endIndex < url.length(); endIndex++) {
                if ((url.charAt(endIndex) < '0') || (url.charAt(endIndex) > '9')) {
                    break;
                }
            }
            port = url.substring(index + 1, endIndex).trim();
        }

        index = url.indexOf(stringUser);
        String userName;
        if (index == -1) {
            //try to get userName from connectProperty
            if ((userName = connectProperty.getProperty(stringUser)) == null) {
                userName = auth.user; //userName not in url or property file, get it from auth.user
                connectProperty.setProperty(stringUser, userName);
            }
        } else {
            for (endIndex = index + 1; endIndex < url.length(); endIndex++) {
                if (url.charAt(endIndex) == '&') {
                    break;
                }
            }

            //skip "user=" then get real userName
            userName = url.substring(index + 1 + stringUser.length(), endIndex).trim();
        }

        properties.setProperty(stringUser, userName);
        properties.setProperty(stringIp, ip);
        properties.setProperty(stringPort, port);
    }

    /**
     * 设置了无密码登陆的情况下把客户端传过来的用户账号改变为默认账户
     * @param auth
     * @param userMaps
     */
	private void setDefaultAccount(AuthPacket auth, Map<String, UserConfig> userMaps) {
		String defaultUser;
		Iterator<UserConfig> items = userMaps.values().iterator();
		while(items.hasNext()){
			UserConfig userConfig = items.next();
			if(userConfig.isDefaultAccount()){
				defaultUser = userConfig.getName(); 
				auth.user = defaultUser;
			}
		}
	}
    
    //TODO: add by zhuam
    //前端 connection 达到该用户设定的阀值后, 立马降级拒绝连接
    protected boolean isDegrade(String user) {
    	
    	int benchmark = source.getPrivileges().getBenchmark(user);
    	if ( benchmark > 0 ) {
    	
	    	int forntedsLength = 0;
	    	NIOProcessor[] processors = MycatServer.getInstance().getProcessors();
			for (NIOProcessor p : processors) {
				forntedsLength += p.getForntedsLength();
			}
		
			if ( forntedsLength >= benchmark ) {							
				return true;
			}			
    	}
		
		return false;
    }
    
    protected boolean checkUser(String user, String host) {
        return source.getPrivileges().userExists(user, host);
    }

    protected boolean checkPassword(byte[] password, String user) {
        String pass = source.getPrivileges().getPassword(user);

        // check null
        if (pass == null || pass.length() == 0) {
            if (password == null || password.length == 0) {
                return true;
            } else {
                return false;
            }
        }
        if (password == null || password.length == 0) {
            return false;
        }

        // encrypt
        byte[] encryptPass = null;
        try {
            encryptPass = SecurityUtil.scramble411(pass.getBytes(), source.getSeed());
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn(source.toString(), e);
            return false;
        }
        if (encryptPass != null && (encryptPass.length == password.length)) {
            int i = encryptPass.length;
            while (i-- != 0) {
                if (encryptPass[i] != password[i]) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }

    protected int checkSchema(String schema, String user) {
        if (schema == null) {
            return 0;
        }
        FrontendPrivileges privileges = source.getPrivileges();
        if (!privileges.schemaExists(schema)) {
            return ErrorCode.ER_BAD_DB_ERROR;
        }
        Set<String> schemas = privileges.getUserSchemas(user);
        if (schemas == null || schemas.size() == 0 || schemas.contains(schema)) {
            return 0;
        } else {
            return ErrorCode.ER_DBACCESS_DENIED_ERROR;
        }
    }

    protected void success(AuthPacket auth) {
        //set catalog after authen
        source.catalog = auth.database;

        source.setAuthenticated(true);
        source.setUser(auth.user);
        source.setCatalog(auth.database);
        source.setSchema(auth.connectAttrs.get(serverSchema));
        source.setCharsetIndex(auth.charsetIndex);
        source.setHandler(new FrontendCommandHandler(source));

        if (LOGGER.isInfoEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(source).append('\'').append(auth.user).append("' login success");
            byte[] extra = auth.extra;
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.info(s.toString());
        }

        ByteBuffer buffer = source.allocate();
        source.write(source.writeToBuffer(AUTH_OK, buffer));
        boolean clientCompress = Capabilities.CLIENT_COMPRESS==(Capabilities.CLIENT_COMPRESS & auth.clientFlags);
        boolean usingCompress= MycatServer.getInstance().getConfig().getSystem().getUseCompression()==1 ;
        if(clientCompress&&usingCompress)
        {
            source.setSupportCompress(true);
        }
    }

    protected void failure(int errno, String info) {
        LOGGER.error(source.toString() + info);
        int maxLength = (info.length() > maxMessageLen)? maxMessageLen: info.length();
        source.writeErrMessage((byte) 2, errno, info.substring(0, (maxLength - 1)));
    }

}
