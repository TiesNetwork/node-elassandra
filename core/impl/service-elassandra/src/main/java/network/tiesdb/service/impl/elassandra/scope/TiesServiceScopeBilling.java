package network.tiesdb.service.impl.elassandra.scope;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.service.scope.api.TiesEntryExtended;
import network.tiesdb.service.scope.api.TiesServiceScopeAction;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query;

public class TiesServiceScopeBilling {

    private static final Logger LOG = LoggerFactory.getLogger(TiesServiceScopeBilling.class);

    public final class Billing {

        private BigInteger total = BigInteger.ZERO;

        private Billing() {
        }

        public synchronized boolean isEmpty() {
            return total.compareTo(BigInteger.ZERO) == 0;
        }

        public boolean isBalanced() {
            // TODO cheques amount should be equal to total billed amount
            // TODO Add cheque calculations. Accepting empty total only for now.
            return isEmpty();
        }

        public synchronized void addEntry(TiesEntryExtended entry) throws TiesServiceScopeException {
            //total = total.add(BigInteger.TEN);
        }

        public synchronized void addQuery(Query query) throws TiesServiceScopeException {
            //total = total.add(BigInteger.ONE);
        }

    }

    interface PaidAction extends TiesServiceScopeAction {
        Billing getBilling();
    }

    public <T extends PaidAction> T checkActionBillingBlank(T action) throws TiesServiceScopeException {
        if (!getBillingSafe(action).isEmpty()) {
            throw new TiesServiceScopeException(
                    "Action billing is not empty. Meybe action is processed more than once or wrong billing information used for processing.");
        }
        return action;
    }

    public <T extends PaidAction> T checkActionBillingTotal(T action) throws TiesServiceScopeException {
        if (!getBillingSafe(action).isBalanced()) {
            // TODO Add billing information to action
            throw new TiesServiceScopeException("Action billing failed! Not all action entities has cheques!");
            //LOG.error("Action billing failed! Not all action entities has cheques!");
        }
        return action;
    }

    private Billing getBillingSafe(PaidAction action) throws TiesServiceScopeException {
        Billing billing = action.getBilling();
        if (null == billing) {
            throw new TiesServiceScopeException("Action billing failed. Action is wrong or action action payment is incomplete.");
        }
        return billing;
    }

    public Billing newBilling() {
        return new Billing();
    }

}
