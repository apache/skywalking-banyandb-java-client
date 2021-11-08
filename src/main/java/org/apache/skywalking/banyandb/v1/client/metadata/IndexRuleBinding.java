package org.apache.skywalking.banyandb.v1.client.metadata;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.skywalking.banyandb.database.v1.metadata.BanyandbMetadata;
import org.apache.skywalking.banyandb.v1.Banyandb;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@EqualsAndHashCode
public class IndexRuleBinding implements Schema<BanyandbMetadata.IndexRuleBinding> {
    /**
     * name of the IndexRuleBinding
     */
    private final String name;

    private List<String> rules;

    private final Subject subject;

    private ZonedDateTime beginAt;

    private ZonedDateTime expireAt;

    /**
     * last updatedAt timestamp
     * This field can only be set by the server
     */
    @EqualsAndHashCode.Exclude
    private final ZonedDateTime updatedAt;

    public IndexRuleBinding(String name, Subject subject) {
        this(name, subject, null);
    }

    private IndexRuleBinding(String name, Subject subject, ZonedDateTime updatedAt) {
        this.name = name;
        this.rules = new ArrayList<>();
        this.subject = subject;
        this.updatedAt = updatedAt;
    }

    public IndexRuleBinding addRule(String ruleName) {
        this.rules.add(ruleName);
        return this;
    }

    @Override
    public BanyandbMetadata.IndexRuleBinding serialize(String group) {
        return BanyandbMetadata.IndexRuleBinding.newBuilder()
                .setMetadata(Banyandb.Metadata.newBuilder().setName(name).setGroup(group).build())
                .addAllRules(this.rules)
                .setSubject(this.subject.serialize())
                .build();
    }

    public static IndexRuleBinding fromProtobuf(BanyandbMetadata.IndexRuleBinding pb) {
        IndexRuleBinding indexRuleBinding = new IndexRuleBinding(pb.getMetadata().getName(), Subject.fromProtobuf(pb.getSubject()), null);
        indexRuleBinding.setRules(new ArrayList<>(pb.getRulesList()));
        return indexRuleBinding;
    }

    @RequiredArgsConstructor
    @Getter
    public static class Subject implements Serializable<BanyandbMetadata.Subject> {
        private final String name;
        private final Catalog catalog;

        @Override
        public BanyandbMetadata.Subject serialize() {
            return BanyandbMetadata.Subject.newBuilder()
                    .setName(this.name)
                    .setCatalog(this.catalog.getCatalog())
                    .build();
        }

        public static Subject newStream(final String name) {
            return new Subject(name, Catalog.STREAM);
        }

        public static Subject newMeasure(final String name) {
            return new Subject(name, Catalog.MEASURE);
        }

        private static Subject fromProtobuf(BanyandbMetadata.Subject pb) {
            switch (pb.getCatalog()) {
                case CATALOG_STREAM:
                    return newStream(pb.getName());
                case CATALOG_MEASURE:
                    return newMeasure(pb.getName());
                default:
                    throw new IllegalArgumentException("unrecognized catalog");
            }
        }
    }
}
