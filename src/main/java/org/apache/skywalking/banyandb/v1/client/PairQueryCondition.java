/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.banyandb.v1.client;

import org.apache.skywalking.banyandb.v1.Banyandb;

import java.util.List;

/**
 * PairQuery represents a query condition, including tag name, operator, and value(s);
 */
public abstract class PairQueryCondition<T> extends TagAndValue<T> {
    protected final Banyandb.Condition.BinaryOp op;

    private PairQueryCondition(String tagFamilyName, String tagName, Banyandb.Condition.BinaryOp op, T value) {
        super(tagFamilyName, tagName, value);
        this.op = op;
    }

    Banyandb.Condition build() {
        return Banyandb.Condition.newBuilder()
                .setName(this.tagName)
                .setOp(this.op)
                .setValue(buildTypedPair()).build();
    }

    /**
     * The various implementations should build different TypedPair
     *
     * @return typedPair to be included
     */
    protected abstract Banyandb.TagValue buildTypedPair();

    /**
     * LongQueryCondition represents `tag(Long) $op value` condition.
     */
    public static class LongQueryCondition extends PairQueryCondition<Long> {
        private LongQueryCondition(String tagFamilyName, String tagName, Banyandb.Condition.BinaryOp op, Long value) {
            super(tagFamilyName, tagName, op, value);
        }

        @Override
        protected Banyandb.TagValue buildTypedPair() {
            return Banyandb.TagValue.newBuilder()
                    .setInt(Banyandb.Int
                            .newBuilder()
                            .setValue(value).build())
                    .build();
        }

        /**
         * Build a query condition for {@link Long} type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_EQ} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `Long == value`
         */
        public static PairQueryCondition<Long> eq(String tagFamilyName, String tagName, Long val) {
            return new LongQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_EQ, val);
        }

        /**
         * Build a query condition for {@link Long} type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_NE} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `Long != value`
         */
        public static PairQueryCondition<Long> ne(String tagFamilyName, String tagName, Long val) {
            return new LongQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_NE, val);
        }

        /**
         * Build a query condition for {@link Long} type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_GT} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `Long &gt; value`
         */
        public static PairQueryCondition<Long> gt(String tagFamilyName, String tagName, Long val) {
            return new LongQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_GT, val);
        }

        /**
         * Build a query condition for {@link Long} type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_GE} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `Long &ge; value`
         */
        public static PairQueryCondition<Long> ge(String tagFamilyName, String tagName, Long val) {
            return new LongQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_GE, val);
        }

        /**
         * Build a query condition for {@link Long} type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_LT} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `Long &lt; value`
         */
        public static PairQueryCondition<Long> lt(String tagFamilyName, String tagName, Long val) {
            return new LongQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_LT, val);
        }

        /**
         * Build a query condition for {@link Long} type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_LE} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `Long &le; value`
         */
        public static PairQueryCondition<Long> le(String tagFamilyName, String tagName, Long val) {
            return new LongQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_LE, val);
        }
    }

    /**
     * StringQueryCondition represents `tag(String) $op value` condition.
     */
    public static class StringQueryCondition extends PairQueryCondition<String> {
        private StringQueryCondition(String tagFamilyName, String tagName, Banyandb.Condition.BinaryOp op, String value) {
            super(tagFamilyName, tagName, op, value);
        }

        @Override
        protected Banyandb.TagValue buildTypedPair() {
            return Banyandb.TagValue.newBuilder()
                    .setStr(Banyandb.Str
                            .newBuilder()
                            .setValue(value).build())
                    .build();
        }

        /**
         * Build a query condition for {@link String} type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_EQ} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `String == value`
         */
        public static PairQueryCondition<String> eq(String tagFamilyName, String tagName, String val) {
            return new StringQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_EQ, val);
        }

        /**
         * Build a query condition for {@link String} type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_NE} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `String != value`
         */
        public static PairQueryCondition<String> ne(String tagFamilyName, String tagName, String val) {
            return new StringQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_NE, val);
        }
    }

    /**
     * StringArrayQueryCondition represents `tag(List of String) $op value` condition.
     */
    public static class StringArrayQueryCondition extends PairQueryCondition<List<String>> {
        private StringArrayQueryCondition(String tagFamilyName, String tagName, Banyandb.Condition.BinaryOp op, List<String> value) {
            super(tagFamilyName, tagName, op, value);
        }

        @Override
        protected Banyandb.TagValue buildTypedPair() {
            return Banyandb.TagValue.newBuilder()
                    .setStrArray(Banyandb.StrArray
                            .newBuilder()
                            .addAllValue(value).build())
                    .build();
        }

        /**
         * Build a query condition for {@link List} of {@link String} as the type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_EQ} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `[String] == values`
         */
        public static PairQueryCondition<List<String>> eq(String tagFamilyName, String tagName, List<String> val) {
            return new StringArrayQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_EQ, val);
        }

        /**
         * Build a query condition for {@link List} of {@link String} as the type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_NE} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `[String] != values`
         */
        public static PairQueryCondition<List<String>> ne(String tagFamilyName, String tagName, List<String> val) {
            return new StringArrayQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_NE, val);
        }

        /**
         * Build a query condition for {@link List} of {@link String} as the type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_HAVING} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `[String] having values`
         */
        public static PairQueryCondition<List<String>> having(String tagFamilyName, String tagName, List<String> val) {
            return new StringArrayQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_HAVING, val);
        }

        /**
         * Build a query condition for {@link List} of {@link String} as the type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_NOT_HAVING} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `[String] not having values`
         */
        public static PairQueryCondition<List<String>> notHaving(String tagFamilyName, String tagName, List<String> val) {
            return new StringArrayQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_NOT_HAVING, val);
        }
    }

    /**
     * LongArrayQueryCondition represents `tag(List of Long) $op value` condition.
     */
    public static class LongArrayQueryCondition extends PairQueryCondition<List<Long>> {
        private LongArrayQueryCondition(String tagFamilyName, String tagName, Banyandb.Condition.BinaryOp op, List<Long> value) {
            super(tagFamilyName, tagName, op, value);
        }

        @Override
        protected Banyandb.TagValue buildTypedPair() {
            return Banyandb.TagValue.newBuilder()
                    .setIntArray(Banyandb.IntArray
                            .newBuilder()
                            .addAllValue(value).build())
                    .build();
        }

        /**
         * Build a query condition for {@link List} of {@link Long} as the type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_EQ} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `[Long] == value`
         */
        public static PairQueryCondition<List<Long>> eq(String tagFamilyName, String tagName, List<Long> val) {
            return new LongArrayQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_EQ, val);
        }

        /**
         * Build a query condition for {@link List} of {@link Long} as the type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_NE} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `[Long] != value`
         */
        public static PairQueryCondition<List<Long>> ne(String tagFamilyName, String tagName, List<Long> val) {
            return new LongArrayQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_NE, val);
        }

        /**
         * Build a query condition for {@link List} of {@link Long} as the type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_HAVING} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `[Long] having values`
         */
        public static PairQueryCondition<List<Long>> having(String tagFamilyName, String tagName, List<Long> val) {
            return new LongArrayQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_HAVING, val);
        }

        /**
         * Build a query condition for {@link List} of {@link Long} as the type
         * and {@link Banyandb.Condition.BinaryOp#BINARY_OP_NOT_HAVING} as the relation
         *
         * @param tagFamilyName family name of the tag
         * @param tagName name of the tag
         * @param val       value of the tag
         * @return a query that `[Long] not having values`
         */
        public static PairQueryCondition<List<Long>> notHaving(String tagFamilyName, String tagName, List<Long> val) {
            return new LongArrayQueryCondition(tagFamilyName, tagName, Banyandb.Condition.BinaryOp.BINARY_OP_NOT_HAVING, val);
        }
    }
}
