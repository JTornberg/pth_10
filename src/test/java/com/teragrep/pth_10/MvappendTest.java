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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import java.util.Arrays;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MvappendTest {
    private final String testFile = "src/test/resources/mvappendTest_data*.jsonl";
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
    public void stringAppendInOrder() {
        final String q = "index=index_A | eval a=mvappend(\"Bob\", \"World\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[Bob, World]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void twoMultivalueColumns() {
        final String q = "index=index_A | eval fruits=mvappend(\"mango\", \"apple\") "
                + "| eval berries=mvappend(\"blueberry\", \"strawberry\") | eval all=mvappend(fruits, berries)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions
                    .assertEquals("[mango, apple, blueberry, strawberry]", row.getList(row.fieldIndex("all")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void mvcountOverAppendedValues() {
        final String q = "index=index_A | eval fruits=mvappend(\"apple\", \"banana\") "
                + "| eval berries=mvappend(\"blueberry\", \"lingonberry\") | eval all=mvappend(fruits, berries) | eval c=mvcount(all)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            final Row row = rows.get(0);
            Assertions.assertEquals("4", row.getString(row.fieldIndex("c")));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void nullArgumentIsDropped() {
        final String q = "index=index_A | eval a=mvappend(\"mango\", null())";
        this.streamingTestUtil.performDPLTest(q, testFile, res -> {

            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[mango]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void nestedMvappend() {
        final String q = "index=index_A | eval a=mvappend(mvappend(\"a\", \"b\"), \"c\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[a, b, c]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void numericArgumentBecomesString() {
        final String q = "index=index_A | eval a=mvappend(1, 2)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final StructField a = res.schema().apply("a");
            Assertions.assertEquals(DataTypes.createArrayType(DataTypes.StringType, false), a.dataType());

            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[1, 2]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void singleArgument() {
        final String q = "index=index_A | eval a=mvappend(1)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[1]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void nullColumnReference() {
        final String q = "index=index_A | eval n=null() | eval a=mvappend(\"mango\", n)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[mango]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void allNullArgument() {
        final String q = "index=index_A | eval a=mvappend(null(), null())";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[]", row.getList(row.fieldIndex("a")).toString());
            Assertions.assertTrue(row.getList(row.fieldIndex("a")).isEmpty());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void multivalueFromSplit() {
        final String q = "index=index_A | eval a=mvappend(split(\"banana;kiwi\", \";\", \"apple\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[banana, kiwi, apple]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void multivalueFromMvdedup() {
        final String q = "index=index_A | eval a=mvappend(mvdedup(mvappend((\"kiwi\", \"apple\", \"kiwi\")), \"mango\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[kiwi, apple, mango]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void multivalueFromMvrange() {
        final String q = "index=index_A | eval a=mvappend(mvrange(1, 4, 1), \"mango\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[1, 2, 3, mango]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void mvJoinOverNumericValues() {
        final String q = "index=index_A | eval a=mvjoin(mvappend(1, 2), \",\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("1,2", row.getString(row.fieldIndex("a")));
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void sameMvColumnTwice() {
        final String q = "index=index_A | eval fruits=mvappend(\"apple\", \"kiwi\") | eval a=mvappend(fruits, fruits)";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[apple, kiwi, apple, kiwi]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void multivalueBetweenSingles() {
        final String q = "index=index_A | eval fruits=mvappend(\"apple\", \"banana\") | eval a=mvappend(\"first\", fruits, \"last\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals("[first, apple, banana, last]", row.getList(row.fieldIndex("a")).toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    public void emptyStringIsKept() {
        final String q = "index=index_A | eval a=mvappend(\"\", \"apple\", \"\", \"banana\", \"\")";

        this.streamingTestUtil.performDPLTest(q, testFile, res -> {
            final List<Row> rows = res.collectAsList();
            Assertions.assertEquals(1, rows.size());

            Row row = rows.get(0);
            Assertions.assertEquals(5, row.getList(row.fieldIndex("a")).size());
            Assertions.assertEquals(Arrays.asList("", "apple", "", "banana", ""), row.getList(row.fieldIndex("a")));
        });
    }
}
