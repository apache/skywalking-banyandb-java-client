package org.apache.skywalking.banyandb.v1.client.metadata;

import io.grpc.Channel;
import org.apache.skywalking.banyandb.common.v1.BanyandbCommon;
import org.apache.skywalking.banyandb.property.v1.BanyandbProperty;
import org.apache.skywalking.banyandb.property.v1.PropertyServiceGrpc;
import org.apache.skywalking.banyandb.v1.client.grpc.HandleExceptionsWith;
import org.apache.skywalking.banyandb.v1.client.grpc.exception.BanyanDBException;

import java.util.List;
import java.util.stream.Collectors;

public class PropertyStore {
    private final PropertyServiceGrpc.PropertyServiceBlockingStub stub;

    public PropertyStore(Channel channel) {
        this.stub = PropertyServiceGrpc.newBlockingStub(channel);
    }

    public void create(Property payload) throws BanyanDBException {
        HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.create(BanyandbProperty.CreateRequest.newBuilder()
                        .setProperty(payload.serialize())
                        .build()));
    }

    public void update(Property payload) throws BanyanDBException {
        HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.update(BanyandbProperty.UpdateRequest.newBuilder()
                        .setProperty(payload.serialize())
                        .build()));
    }

    public boolean delete(String group, String name, String id) throws BanyanDBException {
        BanyandbProperty.DeleteResponse resp = HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.delete(BanyandbProperty.DeleteRequest.newBuilder()
                        .setMetadata(BanyandbProperty.Metadata
                                .newBuilder()
                                .setContainer(BanyandbCommon.Metadata.newBuilder()
                                        .setGroup(group)
                                        .setName(name)
                                        .build())
                                .setId(id)
                                .build())
                        .build()));
        return resp != null && resp.getDeleted();
    }

    public Property get(String group, String name, String id) throws BanyanDBException {
        BanyandbProperty.GetResponse resp = HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.get(BanyandbProperty.GetRequest.newBuilder()
                        .setMetadata(BanyandbProperty.Metadata
                                .newBuilder()
                                .setContainer(BanyandbCommon.Metadata.newBuilder()
                                        .setGroup(group)
                                        .setName(name)
                                        .build())
                                .setId(id)
                                .build())
                        .build()));

        return Property.fromProtobuf(resp.getProperty());
    }

    public List<Property> list(String group, String name) throws BanyanDBException {
        BanyandbProperty.ListResponse resp = HandleExceptionsWith.callAndTranslateApiException(() ->
                this.stub.list(BanyandbProperty.ListRequest.newBuilder()
                        .setContainer(BanyandbCommon.Metadata.newBuilder()
                                .setGroup(group)
                                .setName(name)
                                .build())
                        .build()));

        return resp.getPropertyList().stream().map(Property::fromProtobuf).collect(Collectors.toList());
    }
}
