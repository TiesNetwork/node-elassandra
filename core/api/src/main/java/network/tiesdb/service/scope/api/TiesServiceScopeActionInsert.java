package network.tiesdb.service.scope.api;

import java.util.Map;

public interface TiesServiceScopeActionInsert {

    long getEntryVersion();

    String getTablespaceName();

    String getTableName();

    byte[] getHeaderRawBytes();

    Map<String, TiesValue> getFieldValues();

}