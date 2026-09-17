/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2026 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_10;

import com.teragrep.pth_10.ast.MultiValueColumn;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AppendTest {

    private final String testFile = "src/test/resources/appendTest_data*.jsonl";
    private StreamingTestUtil streamingTestUtil;

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
        this.streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void appendStrings() {
        final String q = "index=index_A | eval a=mvappend(\"one\", \"two\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size(), "Should give one row");

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("a"));
            Assertions.assertEquals(2, values.size());
            Assertions.assertEquals("[one, two]", values.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void appendTwoMultivalueColumns() {
        final String q = "index=index_A | eval mv1=mvappend(\"one\", 2) "
                + "| eval mv2=mvappend(3, 4) | eval all=mvappend(mv1, mv2)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("all"));
            Assertions.assertEquals("[one, 2, 3, 4]", values.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void appendStringAndNumber() {
        final String q = "index=index_A | eval a=mvappend(\"one\", 2)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("a"));
            Assertions.assertEquals("[one, 2]", values.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void appendNumbers() {
        final String q = "index=index_A | eval a=mvappend(3, 2) | eval b=mvappend(1, a)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("b"));
            Assertions.assertEquals("[1, 3, 2]", values.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void appendStringAndMultivalue() {
        final String q = "index=index_A | eval mv=mvappend(\"one\", \"two\") | eval a=mvappend(mv, \"three\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("a"));
            Assertions.assertEquals("[one, two, three]", values.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void appendMultivalueAndNumber() {
        final String q = "index=index_A | eval mv=mvappend(\"one\", \"two\") | eval a=mvappend(mv, 3)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("a"));
            Assertions.assertEquals("[one, two, 3]", values.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void nullsOnly() {
        final String q = "index=index_A | eval a=mvappend(null(), null())";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("a"));
            Assertions.assertEquals("[]", values.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void nullInsideAppend() {
        final String q = "index=index_A | eval a=mvappend(\"one\", null(), 2)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("a"));
            Assertions.assertEquals("[one, 2]", values.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void nullArgumentIsDropped() {
        final String q = "index=index_A | eval a=mvappend(1, null())";
        this.streamingTestUtil.performDPLTest(q, testFile, res -> {

            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            List<Object> values = row.getList(row.fieldIndex("a"));
            Assertions.assertEquals("[1]", values.toString());
        });
    }

    @Test
    public void testEqualsContract() {
        EqualsVerifier
                .forClass(MultiValueColumn.class)
                .withPrefabValues(Column.class, new Column("a"), new Column("b"))
                .verify();
    }

    @Test
    public void testNotEquals() {
        MultiValueColumn mvColumn = new MultiValueColumn(new Column("a"));
        MultiValueColumn other = new MultiValueColumn(new Column("b"));

        Assertions.assertNotEquals(other, mvColumn);
    }
}
