package org.apache.skywalking.banyandb.v1.client.metadata;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.skywalking.banyandb.v1.Banyandb;

@RequiredArgsConstructor
public enum Catalog {
    STREAM(Banyandb.Catalog.CATALOG_STREAM), MEASURE(Banyandb.Catalog.CATALOG_MEASURE);

    @Getter(AccessLevel.PACKAGE)
    private final Banyandb.Catalog catalog;
}
