package network.tiesdb.service.scope.api;

public interface TiesValue {

    String getType();

    byte[] getBytes();

    byte[] getFieldFullRawBytes();

}