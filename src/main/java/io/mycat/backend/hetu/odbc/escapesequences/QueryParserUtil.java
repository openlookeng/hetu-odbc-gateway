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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Stack;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.mycat.backend.hetu.odbc.escapesequences.Constants.CHAR_PATTERN;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.LEFT_PARENTHESIS_STRING;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.ODBC_DATE_LITERAL_ESCAPE_SEQUENCE_PATTERN;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.ODBC_INTERVAL_ESCAPE_SEQUENCE_PATTERN;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.ODBC_LIKE_PREDICATE_WITH_ESCAPE_CHARACTER_PATTERN;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.ODBC_SCALAR_FN_PATTERN;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.ODBC_TIMESTAMP_LITERAL_ESCAPE_SEQUENCE_PATTERN;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.ODBC_TIME_LITERAL_ESCAPE_SEQUENCE_PATTERN;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.SQL_CHAR_FILL_TAG;
import static io.mycat.backend.hetu.odbc.escapesequences.Constants.SQL_STRING_TAG;

/**
 * Prepare Query
 * Translating ODBC Escape Sequences to equivalent Hetu syntax.
 * You can view ms's odbc website doc and Hetu's website doc to get more detail about the odbc escape sequences.
 * For now we support: {Date, Time, and Timestamp Escape Sequences}, {Interval Escape Sequences}, {LIKE Escape Sequence},
 * {Scalar Function Escape Sequence}. Most of the scalar functions are supported, but some not.
 * More detail see our Hetu-ODBC doc or the UT code.
 *
 * @since 2020-06-01
 */
public class QueryParserUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParserUtil.class);

    private QueryParserUtil() {
    }

    /**
     * use to parser and rewrite query
     *
     * @param sql the sql string wait to rewrite
     * @return the rewrite result which conform to the Hetu syntax
     */
    public static String parseAndRewriteQuery(String sql) {
        return replaceOdbcEscapeSequences(sql);
    }

    private static String replaceOdbcEscapeSequences(String sql) {
        List<String> tmpCharCache = new ArrayList<>();
        // like pattern must be first
        String likePatternDone = replaceOneLevelPatternLiteral(sql, ODBC_LIKE_PREDICATE_WITH_ESCAPE_CHARACTER_PATTERN, QueryParserUtil::likeEscapeStringHandler);
        String buryCharSql = buryCharPattern(likePatternDone, tmpCharCache);
        String intervalPatternDone = replaceOneLevelPatternLiteral(buryCharSql, ODBC_INTERVAL_ESCAPE_SEQUENCE_PATTERN, QueryParserUtil::intervalEscapeStringHandler);
        String dateLiteralDone = replaceOneLevelPatternLiteral(intervalPatternDone, ODBC_DATE_LITERAL_ESCAPE_SEQUENCE_PATTERN, QueryParserUtil::dateLiteralStringHandler);
        String timeLiteralDone = replaceOneLevelPatternLiteral(dateLiteralDone, ODBC_TIME_LITERAL_ESCAPE_SEQUENCE_PATTERN, QueryParserUtil::timeLiteralStringHandler);
        String timestampLiteralDone = replaceOneLevelPatternLiteral(timeLiteralDone, ODBC_TIMESTAMP_LITERAL_ESCAPE_SEQUENCE_PATTERN, QueryParserUtil::timestampLiteralStringHandler);
        String fnPatternDone = replacePatternFnWithoutChar(timestampLiteralDone);
        return turnBackChar(fnPatternDone, tmpCharCache);
    }

    private static String replaceOneLevelPatternLiteral(String sql, String pattern, Function<String, String> literalHandler) {
        Pattern dateLiteralPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = dateLiteralPattern.matcher(sql);
        StringBuffer builder = new StringBuffer();
        while (matcher.find()) {
            String dateLiteralString = sql.substring(matcher.start(), matcher.end());
            String rewriteStr =  literalHandler.apply(dateLiteralString);
            matcher.appendReplacement(builder, " " + rewriteStr.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$") + " ");
        }
        matcher.appendTail(builder);
        sql = builder.toString();
        return sql;
    }

    // For the prefixed '{' suffixed '}' is the only difference between ODBC Interval Escape String and Hetu sql,
    // We just remove the prefixed '{' suffixed '}'
    private static String intervalEscapeStringHandler(String intervalString) {
        intervalString = intervalString.trim();
        return intervalString.substring(1, intervalString.length()-1);
    }

    private static String likeEscapeStringHandler(String likeString) {
        likeString = likeString.trim();
        Pattern likePattern = Pattern.compile("\\{escape\\s+'.'}", Pattern.CASE_INSENSITIVE);
        Matcher matcher = likePattern.matcher(likeString);
        matcher.find();
        int start = matcher.start();
        int end = matcher.end();
        String escapeStr = likeString.substring(start + 1, end-1);
        return likeString.substring(0, start).trim() + " " + escapeStr;
    }

    private static String dateLiteralStringHandler(String dateLiteral){
        dateLiteral = dateLiteral.trim();
        String charString = dateLiteral.substring(2, dateLiteral.length()-1).trim();
        return "date " + charString;
    }

    private static String timeLiteralStringHandler(String timeLiteral) {
        timeLiteral = timeLiteral.trim();
        String charString = timeLiteral.substring(2, timeLiteral.length()-1).trim();
        return "time " + charString;
    }

    private static String timestampLiteralStringHandler(String timestampLiteral) {
        timestampLiteral = timestampLiteral.trim();
        String charString = timestampLiteral.substring(3, timestampLiteral.length()-1).trim();
        return "timestamp " + charString;
    }

    private static String replacePatternFnWithoutChar(String sql) {
        Pattern fnPattern = Pattern.compile(ODBC_SCALAR_FN_PATTERN, Pattern.CASE_INSENSITIVE);
        while (fnPattern.matcher(sql).find()) {
            Matcher matcher = fnPattern.matcher(sql);
            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                String fnString = sql.substring(matcher.start(), matcher.end());
                matcher.appendReplacement(sb, " " + fnFunctionStringHandler(fnString).replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$") + " ");
            }
            matcher.appendTail(sb);
            // LOGGER.info(sb.toString());
            sql = sb.toString();
        }
        return sql;
    }

    // in 'fn' level grammar only has one function
    private static String fnFunctionStringHandler(String fnString) {
        fnString = fnString.trim();
        String excludeFn = fnString.substring(3, fnString.length() - 1);
        // considering the nested functions
        return fnFunctionHelper(excludeFn);
    }

    private static String fnFunctionHelper(String buryCharFunctionString) {
        buryCharFunctionString = buryCharFunctionString.trim();
        int beginIndexOfFirstParenthesis = buryCharFunctionString.indexOf(LEFT_PARENTHESIS_STRING);
        String functionName;
        String functionVariablesString;
        if (beginIndexOfFirstParenthesis == -1) {
            return buryCharFunctionString;
        } else {
            functionName = buryCharFunctionString.substring(0, beginIndexOfFirstParenthesis).trim();
            functionVariablesString =
                    buryCharFunctionString.substring(beginIndexOfFirstParenthesis + 1, buryCharFunctionString.length() - 1)
                            .trim();
        }
        // LOGGER.info("functionName: " + functionName + " functionVariablesString: " + functionVariablesString);
        List<String> vars = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();
        char[] chs = functionVariablesString.toCharArray();
        Stack<Character> st = new Stack<>();
        for (int i = 0; i < chs.length; i++) {
            switch (chs[i]) {
                case ',':
                    if (st.empty()) {
                        indexes.add(i);
                    }
                    break;
                case '(':
                    st.push(chs[i]);
                    break;
                case ')':
                    if (st.empty()) {
                        // error sql handle
                        return buryCharFunctionString;
                    }
                    if (st.peek() == '(') {
                        st.pop();
                    } else {
                        // error sql handle
                        return buryCharFunctionString;
                    }
                    break;
                default:
            }
        }
        if (!st.empty()) {
            // error sql handle
            return buryCharFunctionString;
        }
        int begin = 0;
        indexes.add(chs.length);
        for (int i = 0; i < indexes.size(); i++) {
            int commaIndex = indexes.get(i);
            String var;
            if (begin != 0) {
                var = functionVariablesString.substring(begin + 1, commaIndex).trim();
            } else {
                var = functionVariablesString.substring(begin, commaIndex).trim();
            }
            var = var.trim();
            if (var.length() != 0) {
                vars.add(fnFunctionHelper(var));
            }
            begin = commaIndex;
        }

        return finalFunctionsTranslator(functionName, vars);
    }

    private static String finalFunctionsTranslator(String functionSignature, List<String> varsList) {
        functionSignature = functionSignature.trim().toUpperCase(Locale.ENGLISH);
        switch (functionSignature) {
            // firstly, rewrite string function
            case "ASCII":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "codepoint(cast(substr(" + varsList.get(0) + ",1,1) as varchar(1)))";
            case "BIT_LENGTH":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "length(" + varsList.get(0) + ")*8";
            case "CHAR":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "chr(" + varsList.get(0) + ")";
            case "CHAR_LENGTH":
            case "CHARACTER_LENGTH":
            case "OCTET_LENGTH":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "length(" + varsList.get(0) + ")";
            case "CONCAT":
                if (varsList.size() != 2) {
                    return primaryFunction(functionSignature, varsList);
                }
                return functionSignature + "(" + varsList.get(0) + ", " + varsList.get(1) + ")";
            case "LCASE":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "lower(" + varsList.get(0) + ")";
            case "LEFT":
                if (varsList.size() != 2) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "substr(" + varsList.get(0) + ", " + varsList.get(1) + ", 1)";
            case "LENGTH":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "length(rtrim(" + varsList.get(0) + "))";
            case "LOCATE":
                if (varsList.size() == 2) {
                    return "strpos(" + varsList.get(0) + ", " + varsList.get(1) + ")";
                } else if (varsList.size() == 3) {
                    return "(strpos(substr(" + varsList.get(0) + ", " + varsList.get(2) + " ), " + varsList.get(1)
                            + ") + " + varsList.get(2) + "-1)";
                } else {
                    return primaryFunction(functionSignature, varsList);
                }
            case "LTRIM":
            case "POSITION":
            case "RTRIM":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return functionSignature + "(" + varsList.get(0) + ")";
            case "REPEAT":
                if (varsList.size() != 2) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "rpad(" + varsList.get(0) + ", " + varsList.get(1) + "*length(" + varsList.get(0) + "), " + varsList.get(0) + ")";
            case "REPLACE":
                if (varsList.size() != 3) {
                    return primaryFunction(functionSignature, varsList);
                }
                return functionSignature + "(" + varsList.get(0) + ", " + varsList.get(1) + ", " + varsList.get(2) + ")";
            case "RIGHT":
                if (varsList.size() != 2) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "substr(" + varsList.get(0) + ", length(" + varsList.get(0) + ") - " + varsList.get(1) + " + 1, 1)";
            case "SPACE":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "rpad(' ', " + varsList.get(0) + ", ' ')";
            case "SUBSTRING":
                if (varsList.size() != 3) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "substr(" + varsList.get(0) + ", " + varsList.get(1) + ", " + varsList.get(2) + ")";
            case "UCASE":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "upper( " + varsList.get(0) + ")";
            // secondly, numeric functions
            case "ABS":
            case "ACOS":
            case "ASIN":
            case "ATAN":
            case "CEILING":
            case "COS":
            case "DEGREES":
            case "EXP":
            case "FLOOR":
            case "RADIANS":
            case "SIGN":
            case "SIN":
            case "SQRT":
            case "TAN":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return functionSignature + "(" + varsList.get(0) + ")";
            case "MOD":
            case "POWER":
            case "ROUND":
                if (varsList.size() != 2) {
                    return primaryFunction(functionSignature, varsList);
                }
                return functionSignature + "(" + varsList.get(0) + ", " + varsList.get(1) + ")";
            case "ATAN2":
                if (varsList.size() != 2) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "atan2(" + varsList.get(1) + ", " + varsList.get(0) + ")";
            case "LOG":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "ln(" + varsList.get(0) + ")";
            case "LOG10":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "log10(" + varsList.get(0) + ")";
            case "PI":
                if (varsList.size() != 0) {
                    return primaryFunction(functionSignature, varsList);
                }
                return functionSignature + "()";
            case "RAND":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "random(" + varsList.get(0)+")";
            // third, the date functions
            case "CURRENT_DATE":
            case "CURDATE":
                if (varsList.size() != 0) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "CURRENT_DATE";
            case "CURRENT_TIME":
            case "CURTIME":
                if (varsList.size() != 0) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "cast(current_time as time)";
            case "CURRENT_TIMESTAMP":
                if (varsList.size() != 0) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "cast(current_timestamp as timestamp)";
            case "DAYOFMONTH":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "day_of_month(" + varsList.get(0) + ")";
            case "DAYOFWEEK":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "day_of_week("+ varsList.get(0)+ ")";
            case "DAYOFYEAR":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "day_of_year("+ varsList.get(0)+")";
            case "EXTRACT":
            case "HOUR":
            case "MINUTE":
            case "MONTH":
            case "QUARTER":
            case "SECOND":
            case "WEEK":
            case "YEAR":
                if (varsList.size() != 1) {
                    return primaryFunction(functionSignature, varsList);
                }
                return functionSignature + "(" + varsList.get(0) + ")";
            case "NOW":
                if (varsList.size() != 0) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "cast(now() as timestamp)";
            case "TIMESTAMPDIFF":
                if (varsList.size() != 3) {
                    return primaryFunction(functionSignature, varsList);
                }
                switch (varsList.get(0)) {
                    case "SQL_TSI_FRAC_SECOND":
                        return "date_diff('millisecond', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    case "SQL_TSI_SECOND":
                        return "date_diff('second', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    case "SQL_TSI_MINUTE":
                        return "date_diff('MINUTE', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    case "SQL_TSI_HOUR":
                        return "date_diff('HOUR', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    case "SQL_TSI_DAY":
                        return "date_diff('day', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    case "SQL_TSI_WEEK":
                        return "date_diff('week', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    case "SQL_TSI_MONTH":
                        return "date_diff('month', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    case "SQL_TSI_QUARTER":
                        return "date_diff('QUARTER', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    case "SQL_TSI_YEAR":
                        return "date_diff('year', " + varsList.get(1) + ", " + varsList.get(2) + ")";
                    default:
                        // error var list
                        return primaryFunction(functionSignature, varsList);
                }
            case "TIMESTAMPADD":
            {
                if (varsList.size() != 3) {
                    return primaryFunction(functionSignature, varsList);
                }
                String var0 = varsList.get(0);
                String var1 = varsList.get(1);
                String var2 = varsList.get(2);
                switch (var0) {
                    case "SQL_TSI_FRAC_SECOND":
                        return "date_add('millisecond', " + var1 + ", " + var2 + ")";
                    case "SQL_TSI_SECOND":
                        return "date_add('second', " + var1 + ", " + var2 + ")";
                    case "SQL_TSI_MINUTE":
                        return "date_add('minute', " + var1 + ", " + var2 + ")";
                    case "SQL_TSI_HOUR":
                        return "date_add('hour', " + var1 + ", " + var2 + ")";
                    case "SQL_TSI_DAY":
                        return "date_add('day', " + var1 + ", " + var2 + ")";
                    case "SQL_TSI_WEEK":
                        return "date_add('week', " + var1 + ", " + var2 + ")";
                    case "SQL_TSI_MONTH":
                        return "date_add('month', " + var1 + ", " + var2 + ")";
                    case "SQL_TSI_QUARTER":
                        return "date_add('quarter', " + var1 + ", " + var2 + ")";
                    case "SQL_TSI_YEAR":
                        return "date_add('year', " + var1 + ", " + var2 + ")";
                    default:
                        // error var list
                        return primaryFunction(functionSignature, varsList);
                }
            }
            // system function
            case "IFNULL":
                if (varsList.size() != 2) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "coalesce(" + varsList.get(0) + ", " + varsList.get(1) + ")";
            case "USER":
                if (varsList.size() != 0) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "current_user";
            // convert odbc function
            case "CONVERT":
                if (varsList.size() != 2) {
                    return primaryFunction(functionSignature, varsList);
                }
                return "cast("+ varsList.get(0) + " as " + toHetuType(varsList.get(1)) + ")";
            default:
                return primaryFunction(functionSignature, varsList);
        }
    }

    private static String primaryFunction(String functionSignature, List<String> varsList) {
        StringBuilder varsT = new StringBuilder();
        for (int i=0; i<varsList.size(); i++) {
            if (i == 0) {
                varsT.append(varsList.get(i));
            } else {
                varsT.append(", ").append(varsList.get(i));
            }
        }
        return functionSignature + "(" + varsT.toString() + ")";
    }

    // for we have limit the data type to sql 92 in the hetu-driver, so in gateway we do not need to deal with this
    // we just add this for unexpected situation
    private static String toHetuType(String type) {
        String upperTypeName = type.trim().toUpperCase(Locale.ENGLISH);
        switch (upperTypeName) {
            case "SQL_BIGINT":
                return "bigint";
            case "SQL_BIT":
                return "boolean";
            case "SQL_CHAR":
            case "SQL_WCHAR":
                return "char";
            case "SQL_DECIMAL":
                return "decimal";
            case "SQL_DOUBLE":
                return "double";
            case "SQL_FLOAT":
                return "float";
            case "SQL_GUID":
            case "SQL_VARBINARY":
            case "SQL_LONGVARBINARY":
                return "varbinary";
            case "SQL_INTEGER":
                return "integer";
            case "SQL_LONGVARCHAR":
            case "SQL_WVARCHAR":
            case "SQL_WLONGVARCHAR":
            case "SQL_VARCHAR":
                return "varchar";
            case "SQL_REAL":
                return "real";
            case "SQL_SMALLINT":
                return "smallint";
            case "SQL_DATE":
            case "SQL_TYPE_DATE":
                return "date";
            case "SQL_TIME":
            case "SQL_TYPE_TIME":
                return "time";
            case "SQL_TIMESTAMP":
            case "SQL_TYPE_TIMESTAMP":
                return "timestamp";
            case "SQL_TINYINT":
                return "tinyint";
            default:
                return upperTypeName;
        }
    }

    private static String buryCharPattern(String strWithPattern, List<String> strCache) {
        Pattern charFnPattern = Pattern.compile(CHAR_PATTERN, Pattern.CASE_INSENSITIVE);
        Matcher charMatcher = charFnPattern.matcher(strWithPattern);
        String result = strWithPattern;
        if (charMatcher.find()) {
            int start = charMatcher.start();
            int end = charMatcher.end();
            // LOGGER.info(start + " : " + end);
            String prt = strWithPattern.substring(start, end);
            strCache.add(prt);
            String tm = conChar(SQL_CHAR_FILL_TAG, prt.length() - 2, strCache.size()-1);
            String str1 = strWithPattern.substring(0, start);
            String str2 = buryCharPattern(strWithPattern.substring(end), strCache);
            // LOGGER.info(prt);
            result = str1.concat(SQL_STRING_TAG).concat(tm).concat(SQL_STRING_TAG).concat(str2);
        }
        return result;
    }

    private static String turnBackChar(String buryCharSql, List<String> cacheList) {
        String result = buryCharSql;
        for (int i = 0; i < cacheList.size(); i++) {
            int length = cacheList.get(i).length();
            result = result.replaceAll(SQL_STRING_TAG + conChar(SQL_CHAR_FILL_TAG, length - 2, i) + SQL_STRING_TAG, cacheList.get(i).replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$"));
        }
        return result;
    }

    private static String conChar(String str, int length, int count) {
        StringBuilder builder = new StringBuilder();
        builder.append(count);
        for (int i = 0; i < length; i++) {
            builder.append(str);
        }
        return builder.toString();
    }
}
