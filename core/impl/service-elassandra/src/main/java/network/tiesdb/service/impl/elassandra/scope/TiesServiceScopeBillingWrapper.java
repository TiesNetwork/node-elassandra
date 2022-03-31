/**
 * Copyright © 2017 Ties BV
 *
 * This file is part of Ties.DB project.
 *
 * Ties.DB project is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Ties.DB project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with Ties.DB project. If not, see <https://www.gnu.org/licenses/lgpl-3.0>.
 */
package network.tiesdb.service.impl.elassandra.scope;

import java.io.IOException;
import java.math.BigInteger;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.service.impl.elassandra.scope.TiesServiceScopeBilling.Billing;
import network.tiesdb.service.impl.elassandra.scope.TiesServiceScopeBilling.PaidAction;
import network.tiesdb.service.scope.api.TiesEntryExtended;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeAction;
import network.tiesdb.service.scope.api.TiesServiceScopeBillingAction;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeHealingAction;
import network.tiesdb.service.scope.api.TiesServiceScopeModificationAction;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollectionAction;
import network.tiesdb.service.scope.api.TiesServiceScopeResultAction;
import network.tiesdb.service.scope.api.TiesServiceScopeSchemaAction;

public class TiesServiceScopeBillingWrapper implements TiesServiceScope {

    private final TiesServiceScope scope;

    private final TiesServiceScopeBilling billing;

    public TiesServiceScopeBillingWrapper(TiesServiceScope scope, TiesServiceScopeBilling billing) {
        this.scope = scope;
        this.billing = billing;
    }

    public void close() throws IOException {
        scope.close();
    }

    public TiesVersion getServiceVersion() {
        return scope.getServiceVersion();
    }

    public void insert(TiesServiceScopeModificationAction action) throws TiesServiceScopeException {
        scope.insert(wrap(action));
    }

    public void update(TiesServiceScopeModificationAction action) throws TiesServiceScopeException {
        scope.update(wrap(action));
    }

    public void delete(TiesServiceScopeModificationAction action) throws TiesServiceScopeException {
        scope.delete(wrap(action));
    }

    public void select(TiesServiceScopeRecollectionAction action) throws TiesServiceScopeException {
        scope.select(wrap(action));
    }

    public void heal(TiesServiceScopeHealingAction action) throws TiesServiceScopeException {
        scope.heal(wrap(action));
    }

    public void schema(TiesServiceScopeSchemaAction action) throws TiesServiceScopeException {
        scope.schema(action);
    }

    public void result(TiesServiceScopeResultAction action) throws TiesServiceScopeException {
        scope.result(action);
    }

    public void billing(TiesServiceScopeBillingAction action) throws TiesServiceScopeException {
        scope.billing(action);
    }

    private TiesServiceScopeModificationAction wrap(TiesServiceScopeModificationAction action) throws TiesServiceScopeException {
        return check(new TiesServiceScopePaidModification(action));
    }

    private TiesServiceScopeRecollectionAction wrap(TiesServiceScopeRecollectionAction action) throws TiesServiceScopeException {
        return check(new TiesServiceScopePaidRecollection(action));
    }

    private TiesServiceScopeHealingAction wrap(TiesServiceScopeHealingAction action) throws TiesServiceScopeException {
        return check(new TiesServiceScopePaidHealing(action));
    }

    private <T extends PaidAction & TiesServiceScopeAction> T check(T action) throws TiesServiceScopeException {
        return billing.checkActionBillingBlank(action);
    }

    private abstract class TiesServiceScopePaidAction implements PaidAction, TiesServiceScopeAction {

        protected final Billing bill;

        public TiesServiceScopePaidAction(BigInteger billingId) {
            this.bill = billing.newBilling(billingId);
        }

        @Override
        public Billing getBilling() {
            return bill;
        }

        protected void checkBilling() throws TiesServiceScopeException {
            billing.checkActionBillingTotal(this);
        }

        protected void aquireBilling() throws TiesServiceScopeException {
            billing.aquireActionBilling(this);
        }

        @Override
        public void checkPrerequisites() throws TiesServiceScopeException {
            aquireBilling();
        }

    }

    private class TiesServiceScopePaidModification extends TiesServiceScopePaidAction implements TiesServiceScopeModificationAction {

        private final TiesServiceScopeModificationAction action;
        private TiesEntryExtended entry;

        public TiesServiceScopePaidModification(TiesServiceScopeModificationAction action) {
            super(action.getMessageId());
            this.action = action;
        }

        public ActionConsistency getConsistency() {
            return action.getConsistency();
        }

        public BigInteger getMessageId() {
            return action.getMessageId();
        }

        public synchronized TiesEntryExtended getEntry() throws TiesServiceScopeException {
            if (null == this.entry) {
                TiesEntryExtended entry = action.getEntry();
                bill.addEntry(entry);
                checkBilling();
                this.entry = entry;
            }
            return this.entry;
        }

        public void setResult(Result result) throws TiesServiceScopeException {
            action.setResult(result);
        }

    }

    private class TiesServiceScopePaidRecollection extends TiesServiceScopePaidAction implements TiesServiceScopeRecollectionAction {

        private final TiesServiceScopeRecollectionAction action;
        private Query query;

        public TiesServiceScopePaidRecollection(TiesServiceScopeRecollectionAction action) {
            super(action.getMessageId());
            this.action = action;
        }

        public ActionConsistency getConsistency() {
            return action.getConsistency();
        }

        public BigInteger getMessageId() {
            return action.getMessageId();
        }

        public synchronized Query getQuery() throws TiesServiceScopeException {
            if (null == this.query) {
                Query query = action.getQuery();
                bill.addQuery(query);
                checkBilling();
                this.query = query;
            }
            return this.query;
        }

        public void setResult(Result result) throws TiesServiceScopeException {
            action.setResult(result);
        }

        public void checkBilling() throws TiesServiceScopeException {
            billing.checkActionBillingTotal(this);
        }

    }

    private class TiesServiceScopePaidHealing extends TiesServiceScopePaidAction implements TiesServiceScopeHealingAction {

        private final TiesServiceScopeHealingAction action;

        public TiesServiceScopePaidHealing(TiesServiceScopeHealingAction action) {
            super(action.getMessageId());
            this.action = action;
        }

        public TiesEntryExtended getEntry() throws TiesServiceScopeException {
            TiesEntryExtended entry = action.getEntry();
            bill.addEntry(entry);
            return entry;
        }

        public void setResult(Result result) throws TiesServiceScopeException {
            action.setResult(checkAndPrepareResult(result));
        }

        public BigInteger getMessageId() {
            return action.getMessageId();
        }

        public Result checkAndPrepareResult(Result result) throws TiesServiceScopeException {
            billing.checkActionBillingTotal(this);
            return result;
        }

    }

}
