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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test Query Parser Util
 *
 * @since 2020-06-03
 */
public class TestQueryParserUtil {
    @Test
    public void testFnStringFunction() {
        // string scalar function
        String fnStrFunction1 = "select {fn ascii(name)}";
        String fnStrFunction2 = "select {fn BIT_LENGTH(name)}";
        String fnStrFunction3 = "select {fn CHAR(name)}";
        String fnStrFunction4 = "select {fn CHAR_LENGTH(name)}";
        String fnStrFunction5 = "select {fn CHARACTER_LENGTH(name)}";
        String fnStrFunction6 = "select {fn CONCAT(name, varchar 'hi it is ok')}";
        String fnStrFunction7 = "select {fn LCASE(name)}";
        String fnStrFunction8 = "select {fn LEFT(name, 3)}";
        String fnStrFunction9 = "select {fn LENGTH(name)}";
        String fnStrFunction10 = "select {fn LOCATE(name, 'She')}";
        String fnStrFunction11 = "select {fn LOCATE(name, 'She', 3)}";
        String fnStrFunction12 = "select {fn LTRIM(name)}";
        String fnStrFunction13 = "select {fn OCTET_LENGTH(name)}";
        String fnStrFunction14 = "select {fn POSITION(name IN 'Gerth is not here')}";
        String fnStrFunction15 = "select {fn REPEAT(name, 9)}";
        String fnStrFunction16 = "select {fn REPLACE(name1, name2, name3)}";
        String fnStrFunction17 = "select {fn RIGHT(name, 110)}";
        String fnStrFunction18 = "select {fn RTRIM(name)}";
        String fnStrFunction19 = "select {fn SPACE(67)}";
        String fnStrFunction20 = "select {fn SUBSTRING(name, 10, 9)}";
        String fnStrFunction21 = "select {fn UCASE(name)}";

        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction1), "select  codepoint(cast(substr(name,1,1) as varchar(1))) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction2), "select  length(name)*8 ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction3), "select  chr(name) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction4), "select  length(name) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction5), "select  length(name) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction6), "select  CONCAT(name, varchar 'hi it is ok') ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction7), "select  lower(name) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction8), "select  substr(name, 3, 1) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction9), "select  length(rtrim(name)) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction10), "select  strpos(name, 'She') ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction11), "select  (strpos(substr(name, 3 ), 'She') + 3-1) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction12), "select  LTRIM(name) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction13), "select  length(name) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction14), "select  POSITION(name IN 'Gerth is not here') ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction15), "select  rpad(name, 9*length(name), name) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction16), "select  REPLACE(name1, name2, name3) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction17), "select  substr(name, length(name) - 110 + 1, 1) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction18), "select  RTRIM(name) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction19), "select  rpad(' ', 67, ' ') ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction20), "select  substr(name, 10, 9) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnStrFunction21), "select  upper( name) ");

        // string scalar function with error arguments
        String fnStrFunction1e = "select {fn ascii(name, name)}";
        String fnStrFunction2e = "select {fn BIT_LENGTH(name, name)}";
        String fnStrFunction3e = "select {fn CHAR(name, name)}";
        String fnStrFunction4e = "select {fn CHAR_LENGTH(name, name)}";
        String fnStrFunction5e = "select {fn CHARACTER_LENGTH(name, name)}";
        String fnStrFunction6e = "select {fn CONCAT(name, name, varchar 'hi it is ok')}";
        String fnStrFunction7e = "select {fn LCASE(name, name)}";
        String fnStrFunction8e = "select {fn LEFT(name, name, 3)}";
        String fnStrFunction9e = "select {fn LENGTH(name, name)}";
        String fnStrFunction10e = "select {fn LOCATE(name, name, 'She')}";
        String fnStrFunction11e = "select {fn LOCATE(name, name, 'She', 3)}";
        String fnStrFunction12e = "select {fn LTRIM(name, name)}";
        String fnStrFunction13e = "select {fn OCTET_LENGTH(name, name)}";
        String fnStrFunction14e = "select {fn POSITION(name, name IN 'Gerth is not here')}";
        String fnStrFunction15e = "select {fn REPEAT(name, name, 9)}";
        String fnStrFunction16e = "select {fn REPLACE(name, name1, name2, name3)}";
        String fnStrFunction17e = "select {fn RIGHT(name, name, 110)}";
        String fnStrFunction18e = "select {fn RTRIM(name, name)}";
        String fnStrFunction19e = "select {fn SPACE(name, 67)}";
        String fnStrFunction20e = "select {fn SUBSTRING(name, name, 10, 9)}";
        String fnStrFunction21e = "select {fn UCASE(name, name)}";

        assertEquals("select  ASCII(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction1e));
        assertEquals("select  BIT_LENGTH(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction2e));
        assertEquals("select  CHAR(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction3e));
        assertEquals("select  CHAR_LENGTH(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction4e));
        assertEquals("select  CHARACTER_LENGTH(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction5e));
        assertEquals("select  CONCAT(name, name, varchar 'hi it is ok') ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction6e));
        assertEquals("select  LCASE(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction7e));
        assertEquals("select  LEFT(name, name, 3) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction8e));
        assertEquals("select  LENGTH(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction9e));
        assertEquals("select  (strpos(substr(name, 'She' ), name) + 'She'-1) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction10e));
        assertEquals("select  LOCATE(name, name, 'She', 3) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction11e));
        assertEquals("select  LTRIM(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction12e));
        assertEquals("select  OCTET_LENGTH(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction13e));
        assertEquals("select  POSITION(name, name IN 'Gerth is not here') ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction14e));
        assertEquals("select  REPEAT(name, name, 9) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction15e));
        assertEquals("select  REPLACE(name, name1, name2, name3) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction16e));
        assertEquals("select  RIGHT(name, name, 110) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction17e));
        assertEquals("select  RTRIM(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction18e));
        assertEquals("select  SPACE(name, 67) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction19e));
        assertEquals("select  SUBSTRING(name, name, 10, 9) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction20e));
        assertEquals("select  UCASE(name, name) ", QueryParserUtil.parseAndRewriteQuery(fnStrFunction21e));
    }

    @Test
    public void testFnNumericFnFunction() {
        // numeric odbc scalar functions
        String fnNumericFunction1 = "select {fn ABS(age)}";
        String fnNumericFunction2 = "select {fn ACOS(age)}";
        String fnNumericFunction3 = "select {fn ASIN(age)}";
        String fnNumericFunction4 = "select {fn ATAN(age)}";
        String fnNumericFunction5 = "select {fn ATAN2(x, y)}";
        String fnNumericFunction6 = "select {fn CEILING(age)}";
        String fnNumericFunction7 = "select {fn COS(age)}";
        String fnNumericFunction8 = "select {fn DEGREES(age)}";
        String fnNumericFunction9 = "select {fn EXP(age)}";
        String fnNumericFunction10 = "select {fn FLOOR(age)}";
        String fnNumericFunction11 = "select {fn LOG(age)}";
        String fnNumericFunction12 = "select {fn LOG10(age)}";
        String fnNumericFunction13 = "select {fn MOD(age,10)}";
        String fnNumericFunction14 = "select {fn PI()}";
        String fnNumericFunction15 = "select {fn POWER(age,10)}";
        String fnNumericFunction16 = "select {fn RADIANS(angle)}";
        String fnNumericFunction17 = "select {fn RAND(age)}";
        String fnNumericFunction18 = "select {fn ROUND(age,10)}";
        String fnNumericFunction19 = "select {fn SIGN(age)}";
        String fnNumericFunction20 = "select {fn SIN(age)}";
        String fnNumericFunction21 = "select {fn SQRT(age)}";
        String fnNumericFunction22 = "select {fn TAN(age)}";
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction1), "select  ABS(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction2), "select  ACOS(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction3), "select  ASIN(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction4), "select  ATAN(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction5), "select  atan2(y, x) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction6), "select  CEILING(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction7), "select  COS(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction8), "select  DEGREES(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction9), "select  EXP(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction10), "select  FLOOR(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction11), "select  ln(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction12), "select  log10(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction13), "select  MOD(age, 10) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction14), "select  PI() ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction15), "select  POWER(age, 10) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction16), "select  RADIANS(angle) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction17), "select  random(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction18), "select  ROUND(age, 10) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction19), "select  SIGN(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction20), "select  SIN(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction21), "select  SQRT(age) ");
        assertEquals( QueryParserUtil.parseAndRewriteQuery(fnNumericFunction22), "select  TAN(age) ");

        // numeric odbc scalar functions
        String fnNumericFunction1e = "select {fn ABS(age, er)}";
        String fnNumericFunction2e = "select {fn ACOS(age, er)}";
        String fnNumericFunction3e = "select {fn ASIN(age, er)}";
        String fnNumericFunction4e = "select {fn ATAN(age, er)}";
        String fnNumericFunction5e = "select {fn ATAN2(x, y, er)}";
        String fnNumericFunction6e = "select {fn CEILING(age, er)}";
        String fnNumericFunction7e = "select {fn COS(age, er)}";
        String fnNumericFunction8e = "select {fn DEGREES(age, er)}";
        String fnNumericFunction9e = "select {fn EXP(age, er)}";
        String fnNumericFunction10e = "select {fn FLOOR(age, er)}";
        String fnNumericFunction11e = "select {fn LOG(age, er)}";
        String fnNumericFunction12e = "select {fn LOG10(age, er)}";
        String fnNumericFunction13e = "select {fn MOD(age,10, er)}";
        String fnNumericFunction14e = "select {fn PI(, er)}";
        String fnNumericFunction15e = "select {fn POWER(age,10, er)}";
        String fnNumericFunction16e = "select {fn RADIANS(angle, er)}";
        String fnNumericFunction17e = "select {fn RAND(age, er)}";
        String fnNumericFunction18e = "select {fn ROUND(age,10, er)}";
        String fnNumericFunction19e = "select {fn SIGN(age, er)}";
        String fnNumericFunction20e = "select {fn SIN(age, er)}";
        String fnNumericFunction21e = "select {fn SQRT(age, er)}";
        String fnNumericFunction22e = "select {fn TAN(age, er)}";
        assertEquals("select  ABS(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction1e));
        assertEquals("select  ACOS(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction2e));
        assertEquals("select  ASIN(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction3e));
        assertEquals("select  ATAN(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction4e));
        assertEquals("select  ATAN2(x, y, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction5e));
        assertEquals("select  CEILING(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction6e));
        assertEquals("select  COS(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction7e));
        assertEquals("select  DEGREES(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction8e));
        assertEquals("select  EXP(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction9e));
        assertEquals("select  FLOOR(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction10e));
        assertEquals("select  LOG(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction11e));
        assertEquals("select  LOG10(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction12e));
        assertEquals("select  MOD(age, 10, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction13e));
        assertEquals("select  PI(, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction14e));
        assertEquals("select  POWER(age, 10, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction15e));
        assertEquals("select  RADIANS(angle, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction16e));
        assertEquals("select  RAND(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction17e));
        assertEquals("select  ROUND(age, 10, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction18e));
        assertEquals("select  SIGN(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction19e));
        assertEquals("select  SIN(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction20e));
        assertEquals("select  SQRT(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction21e));
        assertEquals("select  TAN(age, er) ", QueryParserUtil.parseAndRewriteQuery(fnNumericFunction22e));
    }

    @Test
    public void testTimeDateIntervalFnFunctions() {
        String fnDateTimeIntervalFunction1 = "select {fn CURRENT_DATE()}";
        String fnDateTimeIntervalFunction2 = "select {fn CURRENT_TIME()}";
        String fnDateTimeIntervalFunction3 = "select {fn CURRENT_TIMESTAMP()}";
        String fnDateTimeIntervalFunction4 = "select {fn CURDATE()}";
        String fnDateTimeIntervalFunction5 = "select {fn CURTIME()}";
        String fnDateTimeIntervalFunction6 = "select {fn DAYOFMONTH(date_exp)}";
        String fnDateTimeIntervalFunction7 = "select {fn DAYOFWEEK(date_exp)}";
        String fnDateTimeIntervalFunction8 = "select {fn DAYOFYEAR(date_exp)}";
        String fnDateTimeIntervalFunction9 = "select {fn EXTRACT(extract_field FROM extract_source)}";
        String fnDateTimeIntervalFunction10 = "select {fn HOUR(time_exp)}";
        String fnDateTimeIntervalFunction11 = "select {fn MINUTE(time_exp)}";
        String fnDateTimeIntervalFunction12 = "select {fn MONTH(date_exp)}";
        String fnDateTimeIntervalFunction13 = "select {fn NOW( )}";
        String fnDateTimeIntervalFunction14 = "select {fn QUARTER(date_exp)}";
        String fnDateTimeIntervalFunction15 = "select {fn SECOND(time_exp)}";
        String fnDateTimeIntervalFunction16 = "select {fn TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction17 = "select {fn TIMESTAMPDIFF(SQL_TSI_SECOND, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction18 = "select {fn TIMESTAMPDIFF(SQL_TSI_MINUTE, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction19 = "select {fn TIMESTAMPDIFF(SQL_TSI_HOUR, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction20 = "select {fn TIMESTAMPDIFF(SQL_TSI_DAY, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction21 = "select {fn TIMESTAMPDIFF(SQL_TSI_WEEK, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction22 = "select {fn TIMESTAMPDIFF(SQL_TSI_MONTH, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction23 = "select {fn TIMESTAMPDIFF(SQL_TSI_QUARTER, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction24 = "select {fn TIMESTAMPDIFF(SQL_TSI_YEAR, timestamp_exp1, timestamp_exp2)}";
        String fnDateTimeIntervalFunction25 = "select {fn WEEK(date_exp)}";
        String fnDateTimeIntervalFunction26 = "select {fn YEAR(date_exp) }";
        String fnDateTimeIntervalFunction27 = "select {fn TIMESTAMPADD(SQL_TSI_FRAC_SECOND, integer_exp, HIRE_DATE)} from employee";
        String fnDateTimeIntervalFunction28 = "select {fn TIMESTAMPADD(SQL_TSI_SECOND, integer_exp, HIRE_DATE)} from employee";
        String fnDateTimeIntervalFunction29 = "select {fn TIMESTAMPADD(SQL_TSI_MINUTE, integer_exp, HIRE_DATE)} from employee";
        String fnDateTimeIntervalFunction30 = "select {fn TIMESTAMPADD(SQL_TSI_HOUR, integer_exp, HIRE_DATE)} from employee";
        String fnDateTimeIntervalFunction31 = "select {fn TIMESTAMPADD(SQL_TSI_DAY, integer_exp, HIRE_DATE)} from employee";
        String fnDateTimeIntervalFunction32 = "select {fn TIMESTAMPADD(SQL_TSI_WEEK, integer_exp, HIRE_DATE)} from employee";
        String fnDateTimeIntervalFunction33 = "select {fn TIMESTAMPADD(SQL_TSI_MONTH, integer_exp, HIRE_DATE)} from employee";
        String fnDateTimeIntervalFunction34 = "select {fn TIMESTAMPADD(SQL_TSI_QUARTER, integer_exp, HIRE_DATE)} from employee";
        String fnDateTimeIntervalFunction35 = "select {fn TIMESTAMPADD(SQL_TSI_YEAR, integer_exp, HIRE_DATE)} from employee";

        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction1), "select  CURRENT_DATE ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction2), "select  cast(current_time as time) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction3), "select  cast(current_timestamp as timestamp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction4), "select  CURRENT_DATE ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction5), "select  cast(current_time as time) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction6), "select  day_of_month(date_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction7), "select  day_of_week(date_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction8), "select  day_of_year(date_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction9), "select  EXTRACT(extract_field FROM extract_source) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction10), "select  HOUR(time_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction11), "select  MINUTE(time_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction12), "select  MONTH(date_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction13), "select  cast(now() as timestamp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction14), "select  QUARTER(date_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction15), "select  SECOND(time_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction16), "select  date_diff('millisecond', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction17), "select  date_diff('second', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction18), "select  date_diff('MINUTE', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction19), "select  date_diff('HOUR', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction20), "select  date_diff('day', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction21), "select  date_diff('week', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction22), "select  date_diff('month', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction23), "select  date_diff('QUARTER', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction24), "select  date_diff('year', timestamp_exp1, timestamp_exp2) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction25), "select  WEEK(date_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction26), "select  YEAR(date_exp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction27), "select  date_add('millisecond', integer_exp, HIRE_DATE)  from employee");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction28), "select  date_add('second', integer_exp, HIRE_DATE)  from employee");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction29), "select  date_add('minute', integer_exp, HIRE_DATE)  from employee");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction30), "select  date_add('hour', integer_exp, HIRE_DATE)  from employee");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction31), "select  date_add('day', integer_exp, HIRE_DATE)  from employee");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction32), "select  date_add('week', integer_exp, HIRE_DATE)  from employee");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction33), "select  date_add('month', integer_exp, HIRE_DATE)  from employee");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction34), "select  date_add('quarter', integer_exp, HIRE_DATE)  from employee");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction35), "select  date_add('year', integer_exp, HIRE_DATE)  from employee");

        // error date scalar functions arguments
        String fnDateTimeIntervalFunction1e = "select {fn CURRENT_DATE(er)}";
        String fnDateTimeIntervalFunction2e = "select {fn CURRENT_TIME(er)}";
        String fnDateTimeIntervalFunction3e = "select {fn CURRENT_TIMESTAMP(er)}";
        String fnDateTimeIntervalFunction4e = "select {fn CURDATE(er)}";
        String fnDateTimeIntervalFunction5e = "select {fn CURTIME(er)}";
        String fnDateTimeIntervalFunction6e = "select {fn DAYOFMONTH(date_exp, er)}";
        String fnDateTimeIntervalFunction7e = "select {fn DAYOFWEEK(date_exp, er)}";
        String fnDateTimeIntervalFunction8e = "select {fn DAYOFYEAR(date_exp, er)}";
        String fnDateTimeIntervalFunction9e = "select {fn EXTRACT(extract_field FROM extract_source, er)}";
        String fnDateTimeIntervalFunction10e = "select {fn HOUR(time_exp, er)}";
        String fnDateTimeIntervalFunction11e = "select {fn MINUTE(time_exp, er)}";
        String fnDateTimeIntervalFunction12e = "select {fn MONTH(date_exp, er)}";
        String fnDateTimeIntervalFunction13e = "select {fn NOW( er)}";
        String fnDateTimeIntervalFunction14e = "select {fn QUARTER(date_exp, er)}";
        String fnDateTimeIntervalFunction15e = "select {fn SECOND(time_exp, er)}";
        String fnDateTimeIntervalFunction16e = "select {fn TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction17e = "select {fn TIMESTAMPDIFF(SQL_TSI_SECOND, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction18e = "select {fn TIMESTAMPDIFF(SQL_TSI_MINUTE, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction19e = "select {fn TIMESTAMPDIFF(SQL_TSI_HOUR, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction20e = "select {fn TIMESTAMPDIFF(SQL_TSI_DAY, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction21e = "select {fn TIMESTAMPDIFF(SQL_TSI_WEEK, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction22e = "select {fn TIMESTAMPDIFF(SQL_TSI_MONTH, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction23e = "select {fn TIMESTAMPDIFF(SQL_TSI_QUARTER, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction24e = "select {fn TIMESTAMPDIFF(SQL_TSI_YEAR, timestamp_exp1, timestamp_exp2, er)}";
        String fnDateTimeIntervalFunction25e = "select {fn WEEK(date_exp, er)}";
        String fnDateTimeIntervalFunction26e = "select {fn YEAR(date_exp, er) }";
        String fnDateTimeIntervalFunction27e = "select {fn TIMESTAMPADD(SQL_TSI_FRAC_SECOND, integer_exp, HIRE_DATE, er)} from employee";
        String fnDateTimeIntervalFunction28e = "select {fn TIMESTAMPADD(SQL_TSI_SECOND, integer_exp, HIRE_DATE, er)} from employee";
        String fnDateTimeIntervalFunction29e = "select {fn TIMESTAMPADD(SQL_TSI_MINUTE, integer_exp, HIRE_DATE, er)} from employee";
        String fnDateTimeIntervalFunction30e = "select {fn TIMESTAMPADD(SQL_TSI_HOUR, integer_exp, HIRE_DATE, er)} from employee";
        String fnDateTimeIntervalFunction31e = "select {fn TIMESTAMPADD(SQL_TSI_DAY, integer_exp, HIRE_DATE, er)} from employee";
        String fnDateTimeIntervalFunction32e = "select {fn TIMESTAMPADD(SQL_TSI_WEEK, integer_exp, HIRE_DATE, er)} from employee";
        String fnDateTimeIntervalFunction33e = "select {fn TIMESTAMPADD(SQL_TSI_MONTH, integer_exp, HIRE_DATE, er)} from employee";
        String fnDateTimeIntervalFunction34e = "select {fn TIMESTAMPADD(SQL_TSI_QUARTER, integer_exp, HIRE_DATE, er)} from employee";
        String fnDateTimeIntervalFunction35e = "select {fn TIMESTAMPADD(SQL_TSI_YEAR, integer_exp, HIRE_DATE, er)} from employee";

        assertEquals("select  CURRENT_DATE(er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction1e));
        assertEquals("select  CURRENT_TIME(er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction2e));
        assertEquals("select  CURRENT_TIMESTAMP(er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction3e));
        assertEquals("select  CURDATE(er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction4e));
        assertEquals("select  CURTIME(er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction5e));
        assertEquals("select  DAYOFMONTH(date_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction6e));
        assertEquals("select  DAYOFWEEK(date_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction7e));
        assertEquals("select  DAYOFYEAR(date_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction8e));
        assertEquals("select  EXTRACT(extract_field FROM extract_source, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction9e));
        assertEquals("select  HOUR(time_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction10e));
        assertEquals("select  MINUTE(time_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction11e));
        assertEquals("select  MONTH(date_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction12e));
        assertEquals("select  NOW(er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction13e));
        assertEquals("select  QUARTER(date_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction14e));
        assertEquals("select  SECOND(time_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction15e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction16e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_SECOND, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction17e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_MINUTE, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction18e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_HOUR, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction19e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_DAY, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction20e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_WEEK, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction21e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_MONTH, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction22e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_QUARTER, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction23e));
        assertEquals("select  TIMESTAMPDIFF(SQL_TSI_YEAR, timestamp_exp1, timestamp_exp2, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction24e));
        assertEquals("select  WEEK(date_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction25e));
        assertEquals("select  YEAR(date_exp, er) ", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction26e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_FRAC_SECOND, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction27e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_SECOND, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction28e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_MINUTE, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction29e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_HOUR, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction30e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_DAY, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction31e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_WEEK, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction32e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_MONTH, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction33e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_QUARTER, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction34e));
        assertEquals("select  TIMESTAMPADD(SQL_TSI_YEAR, integer_exp, HIRE_DATE, er)  from employee", QueryParserUtil.parseAndRewriteQuery(fnDateTimeIntervalFunction35e));
    }

    @Test
    public void testSystemFnFunctions() {
        String ifNullFunction = "select {fn IFNULL(exp, values)}";
        String userFunction = "select {fn USER( )}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(ifNullFunction), "select  coalesce(exp, values) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(userFunction), "select  current_user ");

        // error arguments
        String ifNullFunctione = "select {fn IFNULL()}";
        String userFunctione = "select {fn USER(er )}";
        assertEquals("select  IFNULL() ", QueryParserUtil.parseAndRewriteQuery(ifNullFunctione));
        assertEquals("select  USER(er) ",QueryParserUtil.parseAndRewriteQuery(userFunctione));
    }

    @Test
    public void testFnConvert() {
        String convertFunction1 = "select {fn convert(value_exp, SQL_BIGINT)}";
        String convertFunction2 = "select {fn convert(value_exp, SQL_BIT)}";
        String convertFunction3 = "select {fn convert(value_exp, SQL_CHAR)}";
        String convertFunction4 = "select {fn convert(value_exp, SQL_DECIMAL)}";
        String convertFunction5 = "select {fn convert(value_exp, SQL_DOUBLE)}";
        String convertFunction6 = "select {fn convert(value_exp, SQL_FLOAT)}";
        String convertFunction7 = "select {fn convert(value_exp, SQL_GUID)}";
        String convertFunction8 = "select {fn convert(value_exp, SQL_INTEGER)}";
        String convertFunction9 = "select {fn convert(value_exp, SQL_LONGVARBINARY)}";
        String convertFunction10 = "select {fn convert(value_exp, SQL_LONGVARCHAR)}";
        String convertFunction11 = "select {fn convert(value_exp, SQL_REAL)}";
        String convertFunction12 = "select {fn convert(value_exp, SQL_SMALLINT)}";
        String convertFunction13 = "select {fn convert(value_exp, SQL_DATE)}";
        String convertFunction14 = "select {fn convert(value_exp, SQL_TIME)}";
        String convertFunction15 = "select {fn convert(value_exp, SQL_TIMESTAMP)}";
        String convertFunction16 = "select {fn convert(value_exp, SQL_TINYINT)}";
        String convertFunction17 = "select {fn convert(value_exp, SQL_VARBINARY)}";
        String convertFunction18 = "select {fn convert(value_exp, SQL_VARCHAR)}";
        String convertFunction19 = "select {fn convert(value_exp, SQL_WCHAR)}";
        String convertFunction20 = "select {fn convert(value_exp, SQL_WLONGVARCHAR)}";
        String convertFunction21 = "select {fn convert(value_exp, SQL_WVARCHAR)}";

        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction1), "select  cast(value_exp as bigint) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction2), "select  cast(value_exp as boolean) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction3), "select  cast(value_exp as char) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction4), "select  cast(value_exp as decimal) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction5), "select  cast(value_exp as double) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction6), "select  cast(value_exp as float) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction7), "select  cast(value_exp as varbinary) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction8), "select  cast(value_exp as integer) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction9), "select  cast(value_exp as varbinary) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction10), "select  cast(value_exp as varchar) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction11), "select  cast(value_exp as real) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction12), "select  cast(value_exp as smallint) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction13), "select  cast(value_exp as date) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction14), "select  cast(value_exp as time) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction15), "select  cast(value_exp as timestamp) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction16), "select  cast(value_exp as tinyint) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction17), "select  cast(value_exp as varbinary) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction18), "select  cast(value_exp as varchar) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction19), "select  cast(value_exp as char) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction20), "select  cast(value_exp as varchar) ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(convertFunction21), "select  cast(value_exp as varchar) ");

        // error convert function arguments
        String convertFunction1e = "select {fn convert(value_exp, SQL_BIGINT, er)}";
        String convertFunction2e = "select {fn convert(value_exp, SQL_BIT, er)}";
        String convertFunction3e = "select {fn convert(value_exp, SQL_CHAR, er)}";
        String convertFunction4e = "select {fn convert(value_exp, SQL_DECIMAL, er)}";
        String convertFunction5e = "select {fn convert(value_exp, SQL_DOUBLE, er)}";
        String convertFunction6e = "select {fn convert(value_exp, SQL_FLOAT, er)}";
        String convertFunction7e = "select {fn convert(value_exp, SQL_GUID, er)}";
        String convertFunction8e = "select {fn convert(value_exp, SQL_INTEGER, er)}";
        String convertFunction9e = "select {fn convert(value_exp, SQL_LONGVARBINARY, er)}";
        String convertFunction10e = "select {fn convert(value_exp, SQL_LONGVARCHAR, er)}";
        String convertFunction11e = "select {fn convert(value_exp, SQL_REAL, er)}";
        String convertFunction12e = "select {fn convert(value_exp, SQL_SMALLINT, er)}";
        String convertFunction13e = "select {fn convert(value_exp, SQL_DATE, er)}";
        String convertFunction14e = "select {fn convert(value_exp, SQL_TIME, er)}";
        String convertFunction15e = "select {fn convert(value_exp, SQL_TIMESTAMP, er)}";
        String convertFunction16e = "select {fn convert(value_exp, SQL_TINYINT, er)}";
        String convertFunction17e = "select {fn convert(value_exp, SQL_VARBINARY, er)}";
        String convertFunction18e = "select {fn convert(value_exp, SQL_VARCHAR, er)}";
        String convertFunction19e = "select {fn convert(value_exp, SQL_WCHAR, er)}";
        String convertFunction20e = "select {fn convert(value_exp, SQL_WLONGVARCHAR, er)}";
        String convertFunction21e = "select {fn convert(value_exp, SQL_WVARCHAR, er)}";

        assertEquals("select  CONVERT(value_exp, SQL_BIGINT, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction1e));
        assertEquals("select  CONVERT(value_exp, SQL_BIT, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction2e));
        assertEquals("select  CONVERT(value_exp, SQL_CHAR, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction3e));
        assertEquals("select  CONVERT(value_exp, SQL_DECIMAL, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction4e));
        assertEquals("select  CONVERT(value_exp, SQL_DOUBLE, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction5e));
        assertEquals("select  CONVERT(value_exp, SQL_FLOAT, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction6e));
        assertEquals("select  CONVERT(value_exp, SQL_GUID, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction7e));
        assertEquals("select  CONVERT(value_exp, SQL_INTEGER, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction8e));
        assertEquals("select  CONVERT(value_exp, SQL_LONGVARBINARY, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction9e));
        assertEquals("select  CONVERT(value_exp, SQL_LONGVARCHAR, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction10e));
        assertEquals("select  CONVERT(value_exp, SQL_REAL, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction11e));
        assertEquals("select  CONVERT(value_exp, SQL_SMALLINT, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction12e));
        assertEquals("select  CONVERT(value_exp, SQL_DATE, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction13e));
        assertEquals("select  CONVERT(value_exp, SQL_TIME, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction14e));
        assertEquals("select  CONVERT(value_exp, SQL_TIMESTAMP, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction15e));
        assertEquals("select  CONVERT(value_exp, SQL_TINYINT, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction16e));
        assertEquals("select  CONVERT(value_exp, SQL_VARBINARY, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction17e));
        assertEquals("select  CONVERT(value_exp, SQL_VARCHAR, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction18e));
        assertEquals("select  CONVERT(value_exp, SQL_WCHAR, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction19e));
        assertEquals("select  CONVERT(value_exp, SQL_WLONGVARCHAR, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction20e));
        assertEquals("select  CONVERT(value_exp, SQL_WVARCHAR, er) ", QueryParserUtil.parseAndRewriteQuery(convertFunction21e));
    }

    @Test
    public void testDateLiteral() {
        String dateLiteral1 = "{d '2010-10-2'}";
        String dateLiteral2 = "{d '0000-0-0'}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(dateLiteral1), " date '2010-10-2' ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(dateLiteral2), " date '0000-0-0' ");

        // error date literal
        String dateLiteral1e = "{d '2010-10-2''}";
        String dateLiteral2e = "{d '0000-0-0' rre}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(dateLiteral1e), "{d '2010-10-2''}");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(dateLiteral2e), "{d '0000-0-0' rre}");
    }

    @Test
    public void testTimeLiteral() {
        String timeLiteral1 = "{t '01:02:03.456'}";
        String timeLiteral2 = "{t '23:59:59.999'}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(timeLiteral1), " time '01:02:03.456' ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(timeLiteral2), " time '23:59:59.999' ");

        // error literal
        String timeLiteral1e = "{t '01:02:03.456''}";
        String timeLiteral2e = "{t '23:59:59.999' yuyuyi}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(timeLiteral1e), "{t '01:02:03.456''}");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(timeLiteral2e), "{t '23:59:59.999' yuyuyi}");
    }

    @Test
    public void testTimestampLiteral() {
        String timeLiteral1 = "{ts '2011-12-23 01:02:03.456'}";
        String timeLiteral2 = "{ts '2011-12-23 23:59:59.999'}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(timeLiteral1), " timestamp '2011-12-23 01:02:03.456' ");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(timeLiteral2), " timestamp '2011-12-23 23:59:59.999' ");

        // error literal
        String timeLiteral1e = "{ts '2011-12-23 '''01:02:03.456'}";
        String timeLiteral2e = "{ts eerree '2011-12-23 23:59:59.999'}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(timeLiteral1e), "{ts '2011-12-23 '''01:02:03.456'}");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(timeLiteral2e), "{ts eerree '2011-12-23 23:59:59.999'}");
    }

    @Test
    public void testLikeOdbcEscapeSequences() {
        String likePredicate1 = "SELECT Name FROM Customers WHERE Name LIKE '\\%A''$A''A%' {escape '\\'} , name2 LIKE '\\%AAA%' {escape '\\'}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(likePredicate1), "SELECT Name FROM Customers WHERE Name  LIKE '\\%A''$A''A%' escape '\\'  , name2  LIKE '\\%AAA%' escape '\\' ");
        String likePredicate2 = "select dt_varchar from test_tbl_alldatatypes where id in (201,202,203) and dt_varchar like 'testx_stmt_desc_attr' {escape 'x'};";
        assertEquals("select dt_varchar from test_tbl_alldatatypes where id in (201,202,203) and dt_varchar  like 'testx_stmt_desc_attr' escape 'x' ;", QueryParserUtil.parseAndRewriteQuery(likePredicate2));
        String likePredicate3 = "PREPARE STMT4 FROM select dt_varchar from test_tbl_alldatatypes where id in (201,202,203) and dt_varchar like 'testx_stmt_desc_attr' {escape 'x'}";
        assertEquals("PREPARE STMT4 FROM select dt_varchar from test_tbl_alldatatypes where id in (201,202,203) and dt_varchar  like 'testx_stmt_desc_attr' escape 'x' ", QueryParserUtil.parseAndRewriteQuery(likePredicate3));

        // error like pattern
        String e1 = ".... like 'hihii' {escape 'ui'}";
        assertEquals(".... like 'hihii' {escape 'ui'}", QueryParserUtil.parseAndRewriteQuery(e1));
        String e2 = ".... like ss 'hihii''' {escape 'i'}";
        assertEquals(".... like ss 'hihii''' {escape 'i'}", QueryParserUtil.parseAndRewriteQuery(e2));
        String e3 = ".... like 'hih'ii' {escape 'i'}";
        assertEquals(".... like 'hih'ii' {escape 'i'}", QueryParserUtil.parseAndRewriteQuery(e3));
        String e4 = ".... like 'hihii' {escape geee 'i'}";
        assertEquals(".... like 'hihii' {escape geee 'i'}", QueryParserUtil.parseAndRewriteQuery(e4));
    }

    @Test
    public void testIntervalOdbcEscapeSequences() {
        String interval1 = "select  {INTERVAL '3' year to MONTH} +  {INTERVAL '6' year to MONTH}, {INTERVAL '6' MONTH}, {INTERVAL '688' year to MONTH}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(interval1), "select   INTERVAL '3' year to MONTH  +   INTERVAL '6' year to MONTH ,  INTERVAL '6' MONTH ,  INTERVAL '688' year to MONTH ");

        // our parser do not take responsible for INTERVAL verify
        // error pattern
        String e0 = "{INTERVAL '''3' year to MONTH}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(e0), " INTERVAL '''3' year to MONTH ");
        String e1 = "{INTERVAL ''3' year to MONTH}";
        String e2 = "{INTERffVAL '3' year tro MONTH}";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(e1), "{INTERVAL ''3' year to MONTH}");
        assertEquals(QueryParserUtil.parseAndRewriteQuery(e2), "{INTERffVAL '3' year tro MONTH}");
    }

    @Test
    public void testCombineOp() {
        String combine1 = "select {fn DAYOFMONTH( {d '2020-06-05'} )}";
        assertEquals("select  day_of_month(date '2020-06-05') ", QueryParserUtil.parseAndRewriteQuery(combine1));
    }

    @Test
    public void testReplaceOdbcEscapeSequencesWithSql() {
        String sql1 = "SELECT testorders_orc.ee AS ee, '{fn gtr''(dri''ve)}',  {t '00:''00:00'}, testorders_orc.hh AS hh,   " +
                "{fn TIMESTAMPDIFF    (    SQL_TSI_YEAR, {fn CURDATE()}, HIRE_DATE)}, testorders_orc.ii AS ii,  " +
                "CAST({fn TRUNCATE(    EXTRACT(YEAR      FROM testorders_orc.jj),0)} AS INTEGER) AS yr_jj_ok " +
                "FROM hxtest.testorders_orc testorders_orc where Name LIKE '\\%A''$A''A%' {escape '\\'} , Name2 LIKE '\\%AAA%' {escape '\\'} GROUP BY 1,   2,   3,   4";
        String expectedSql1 = "SELECT testorders_orc.ee AS ee, '{fn gtr''(dri''ve)}',   time '00:''00:00' , testorders_orc.hh AS hh,    " +
                "date_diff('year', CURRENT_DATE, HIRE_DATE) , testorders_orc.ii AS ii,  CAST( TRUNCATE(EXTRACT(YEAR      FROM testorders_orc.jj), 0)  AS INTEGER) AS yr_jj_ok " +
                "FROM hxtest.testorders_orc testorders_orc where Name  LIKE '\\%A''$A''A%' escape '\\'  , Name2  LIKE '\\%AAA%' escape '\\'  GROUP BY 1,   2,   3,   4";
        assertEquals(QueryParserUtil.parseAndRewriteQuery(sql1), expectedSql1);
    }

    @Test
    public void testErrorSyntaxFnFunctionSql() {
        String error1 = "select {fn trim(name))} from name_table";
        assertEquals("select  trim(name))  from name_table", QueryParserUtil.parseAndRewriteQuery(error1));
        String error2 = "select {fn trim(name))}} from name_table";
        assertEquals("select  trim(name)) } from name_table", QueryParserUtil.parseAndRewriteQuery(error2));
        String error3 = "select {fn trim(name)) from name_table";
        assertEquals("select {fn trim(name)) from name_table", QueryParserUtil.parseAndRewriteQuery(error3));
    }
}
