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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.exception.TiesConfigurationException;
import network.tiesdb.schema.api.TiesSchema;
import network.tiesdb.service.scope.api.TiesCheque;
import network.tiesdb.service.scope.api.TiesCheque.Address;
import network.tiesdb.service.scope.api.TiesEntryExtended;
import network.tiesdb.service.scope.api.TiesServiceScopeAction;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query;
import network.tiesdb.util.Hex;

public class TiesServiceScopeBilling {

    private static final Logger LOG = LoggerFactory.getLogger(TiesServiceScopeBilling.class);
    private static final BigInteger SLIP0044_BASE = new BigInteger("80000000");
    private static final Pattern HEX_ADDRESS_PATTERN = Pattern.compile("0x[a-fA-F0-9]+");
    private final TiesCheque.Address address;
    private final BigInteger network; // slip-0044 full like 0x8000003c

    public final class Billing {

        private final BigInteger billingId;
        private BigInteger total = BigInteger.ZERO;
        private BigInteger paid = BigInteger.ZERO;

        public Billing(BigInteger billingId) {
            this.billingId = billingId;
        }

        public synchronized boolean isEmpty() {
            return total.compareTo(BigInteger.ZERO) == 0;
        }

        public boolean isBalanced() {
            // TODO cheques amount should be equal to total billed amount
            // TODO Add cheque calculations. Accepting empty total only for now.
            // return total.compareTo(paid) <= 0;
            LOG.debug("Billing {}\n\tTotal: {}\n\tPaid: {}", billingId, total, paid);
            return true; // TODO FIXME implement balance calculation
        }

        public synchronized void addEntry(TiesEntryExtended entry) throws TiesServiceScopeException {

            total = total.add(BigInteger.TEN); // TODO FIXME Change stub price

            paid = paid.add(processCheques(entry.getCheques()));

        }

        public synchronized void addQuery(Query query) throws TiesServiceScopeException {

            total = total.add(BigInteger.ONE); // TODO FIXME Change stub price

            paid = paid.add(processCheques(query.getCheques()));

        }

    }

    interface PaidAction extends TiesServiceScopeAction {
        Billing getBilling();
    }

    public TiesServiceScopeBilling(TiesSchema schema) throws TiesConfigurationException {
        this.network = BigInteger.valueOf(schema.getSchemaNetwork()).add(SLIP0044_BASE);
        this.address = parseNodeAddress(schema.getNodeAddress());
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
            // LOG.error("Action billing failed! Not all action entities has cheques!");
        }
        return action;
    }

    private BigInteger processCheques(Collection<? extends TiesCheque> cheques) {
        return cheques.parallelStream() //
                .filter(ch -> checkCheque(ch)) //
                .map(ch -> ch.getChequeAmount()) //
                .reduce(BigInteger.ZERO, (acc, ch) -> {
                    return acc.add(ch);
                });
    }

    private Billing getBillingSafe(PaidAction action) throws TiesServiceScopeException {
        Billing billing = action.getBilling();
        if (null == billing) {
            throw new TiesServiceScopeException("Action billing failed. Action is wrong or action action payment is incomplete.");
        }
        return billing;
    }

    public Billing newBilling(BigInteger billingId) {
        return new Billing(billingId);
    }

    public boolean checkCheque(TiesCheque ch) {
        BigInteger network = ch.getChequeNetwork();
        if (network.compareTo(SLIP0044_BASE) < 0) {
            network = network.add(SLIP0044_BASE);
        }
        if (network.compareTo(TiesServiceScopeBilling.this.network) != 0)
            return false;
        if (!ch.getChequeAddresses().contains(TiesServiceScopeBilling.this.address)) {
            return false;
        }
        return true;
    }

    private Address parseNodeAddress(String nodeAddressString) throws TiesConfigurationException {
        if (null == nodeAddressString) {
            throw new TiesConfigurationException("Missing node address");
        }
        if (!HEX_ADDRESS_PATTERN.matcher(nodeAddressString).matches()) {
            throw new TiesConfigurationException("Malformed node address: " + nodeAddressString);
        }
        try {
            byte[] nodeAddress = Hex.UPPERCASE_HEX.parseHexBinary(nodeAddressString.substring(2).toUpperCase());
            return new ChequeNodeAddress(nodeAddress);
        } catch (Throwable th) {
            throw new TiesConfigurationException("Malformed node address: " + nodeAddressString, th);
        }

    }

    protected static class ChequeNodeAddress implements TiesCheque.Address {

        private final byte[] address;

        public ChequeNodeAddress(byte[] address) {
            this.address = address;
        }

        @Override
        public byte[] getAddress() {
            return this.address;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(address);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!Address.class.isAssignableFrom(obj.getClass()))
                return false;
            Address other = (Address) obj;
            if (!Arrays.equals(address, other.getAddress()))
                return false;
            return true;
        }

    }

}
