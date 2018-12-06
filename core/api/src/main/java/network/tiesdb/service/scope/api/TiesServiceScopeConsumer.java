package network.tiesdb.service.scope.api;

public interface TiesServiceScopeConsumer {

    void accept(TiesServiceScope serviceScope) throws TiesServiceScopeException;

}
