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

package io.mycat.backend.hetu.odbc.escapesequences;

/**
 * Constants
 *
 * @since 2020-06-01
 */
public class Constants {
    /**
     * ODBC SCALAR FN PATTERN
     */
    public static final String ODBC_SCALAR_FN_PATTERN = "\\{fn[^{}]*}";

    // public static final String CHAR_FN_PATTERN = "'[^']*\\{fn[^']*'";

    /**
     * ODBC DATE LITERAL ESCAPE SEQUENCE PATTERN
     */
    public static final String ODBC_DATE_LITERAL_ESCAPE_SEQUENCE_PATTERN = "\\{d\\s+('[^']*')+}";

    /**
     * ODBC TIME LITERAL ESCAPE SEQUENCE PATTERN
     */
    public static final String ODBC_TIME_LITERAL_ESCAPE_SEQUENCE_PATTERN = "\\{t\\s+('[^']*')+}";

    /**
     * ODBC TIMESTAMP LITERAL ESCAPE SEQUENCE PATTERN
     */
    public static final String ODBC_TIMESTAMP_LITERAL_ESCAPE_SEQUENCE_PATTERN = "\\{ts\\s+('[^']*')+}";

    /**
     * ODBC LIKE PREDICATE WITH ESCAPE CHARACTER PATTERN
     */
    public static final String ODBC_LIKE_PREDICATE_WITH_ESCAPE_CHARACTER_PATTERN = "LIKE\\s+('[^']*')+\\s+\\{escape\\s+'.'}";

    /**
     * ODBC INTERVAL ESCAPE SEQUENCE PATTERN
     */
    public static final String ODBC_INTERVAL_ESCAPE_SEQUENCE_PATTERN = "\\{\\s*INTERVAL\\s+('[^']*')+\\s+[^}]*}";

    /**
     * CHAR PATTERN
     */
    public static final String CHAR_PATTERN = "('[^']*')+";

    /**
     * SPACE PATTERN
     */
    public static final String SPACE_PATTERN = "[\\s\\t\\n\\r]+";

    /**
     * SQL STRING_TAG
     */
    public static final String SQL_STRING_TAG = "'";

    /**
     * SPACE STRING
     */
    public static final String SPACE_STRING = " ";

    /**
     * SQL CHAR FILL TAG
     */
    public static final String SQL_CHAR_FILL_TAG = "#";

    /**
     * LEFT PARENTHESIS STRING
     */
    public static final String LEFT_PARENTHESIS_STRING = "(";

    /**
     * RIGHT PARENTHESIS STRING
     */
    public static final String RIGHT_PARENTHESIS_STRING = ")";

    /**
     * EMPTY STRING
     */
    public static final String EMPTY_STRING = "";

    /**
     * DATE LITERAL HEADER
     */
    public static final String DATE_LITERAL_HEADER = "{d";

    /**
     * TIME LITERAL HEADER
     */
    public static final String TIME_LITERAL_HEADER = "{t";

    /**
     * TIMESTAMP LITERAL HEADER
     */
    public static final String TIMESTAMP_LITERAL_HEADER = "{ts";

    private Constants(){
    }
}
