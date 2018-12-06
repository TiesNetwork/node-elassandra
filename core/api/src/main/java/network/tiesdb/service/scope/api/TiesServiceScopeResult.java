package network.tiesdb.service.scope.api;

public interface TiesServiceScopeResult extends TiesServiceScopeAction {

    interface Result {

        interface Visitor<T> {

            T on(TiesServiceScopeModification.Result result) throws TiesServiceScopeException;

            T on(TiesServiceScopeRecollection.Result result) throws TiesServiceScopeException;

        }

        <T> T accept(Result.Visitor<T> v) throws TiesServiceScopeException;
    }
    
    Result getResult();

}