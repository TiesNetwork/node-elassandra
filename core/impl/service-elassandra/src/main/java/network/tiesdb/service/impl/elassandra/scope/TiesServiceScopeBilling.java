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
import java.nio.ByteBuffer;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.exception.TiesConfigurationException;
import network.tiesdb.schema.api.TiesSchema;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil;
import network.tiesdb.service.scope.api.TiesCheque;
import network.tiesdb.service.scope.api.TiesEntryExtended;
import network.tiesdb.service.scope.api.TiesServiceScopeAction;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query;

public class TiesServiceScopeBilling {

    private static final Logger LOG = LoggerFactory.getLogger(TiesServiceScopeBilling.class);
    public static final BigInteger SLIP0044_BASE = new BigInteger("80000000");

    public static final BigInteger SESSION_CREATE_FEE = BigInteger.valueOf(100);
    public static final BigInteger SESSION_ENTRY_FEE = BigInteger.TEN;
    public static final BigInteger SESSION_QUERY_FEE = BigInteger.ZERO;

    private final TiesSchema schema;

    protected static class ChequeAquiringException extends RuntimeException {

        private static final long serialVersionUID = -4051578732628635680L;
        private final TiesCheque cheque;

        public ChequeAquiringException(TiesCheque cheque, String message, Throwable cause) {
            super(message, cause);
            this.cheque = cheque;
        }

        public ChequeAquiringException(TiesCheque cheque, String message) {
            super(message);
            this.cheque = cheque;
        }

        public TiesCheque getCheque() {
            return cheque;
        }

    }

    protected static interface ChequeMeta {

        BigInteger getChequeNetwork();

        String getTableName();

        String getTablespaceName();

        BigInteger getChequeCropDelta();

        byte[] getSigner();

    }

    protected static class BillingCheque implements TiesCheque {

        private final TiesCheque cheque;
        private final ChequeMeta meta;

        public BillingCheque(TiesCheque cheque, ChequeMeta meta) {
            this.cheque = cheque;
            this.meta = meta;
        }

        public byte[] getSignature() {
            return cheque.getSignature();
        }

        public BigInteger getChequeVersion() {
            return cheque.getChequeVersion();
        }

        public UUID getChequeSession() {
            return cheque.getChequeSession();
        }

        public BigInteger getChequeNumber() {
            return cheque.getChequeNumber();
        }

        public BigInteger getChequeCropAmount() {
            return cheque.getChequeCropAmount();
        }

        public BigInteger getChequeCropDelta() {
            return meta.getChequeCropDelta();
        }

        public byte[] getSigner() {
            byte[] signer = cheque.getSigner();
            return null != signer && signer.length > 0 ? signer : meta.getSigner();
        }

        public BigInteger getChequeNetwork() {
            BigInteger chequeNetwork = cheque.getChequeNetwork();
            chequeNetwork = null != chequeNetwork && !BigInteger.ZERO.equals(chequeNetwork) ? chequeNetwork : meta.getChequeNetwork();
            return 0 > chequeNetwork.compareTo(SLIP0044_BASE) ? chequeNetwork : chequeNetwork.subtract(SLIP0044_BASE);
        }

        public String getTableName() {
            String tableName = cheque.getTableName();
            return null != tableName && !tableName.isEmpty() ? tableName : meta.getTableName();
        }

        public String getTablespaceName() {
            String tablespaceName = cheque.getTablespaceName();
            return null != tablespaceName && !tablespaceName.isEmpty() ? tablespaceName : meta.getTablespaceName();
        }

    }

    public final class Billing {

        private final BigInteger billingId;
        private BigInteger total = BigInteger.ZERO;
        private List<BillingCheque> paidCheques = new ArrayList<>();

        public Billing(BigInteger billingId) {
            this.billingId = billingId;
        }

        public synchronized boolean isEmpty() {
            return total.compareTo(BigInteger.ZERO) == 0;
        }

        public boolean isBalanced() {
            BigInteger paid = getPaid();
            LOG.debug("Billing {}\n\tTotal: {}\n\tPaid: {}", billingId, total, paid);
            return 1 > total.compareTo(paid);
        }

        private BigInteger getPaid() {
            return paidCheques.parallelStream() //
                    .map(ch -> ch.getChequeCropDelta()) //
                    .reduce(BigInteger.ZERO, (acc, ch) -> {
                        return acc.add(ch);
                    });
        }

        public synchronized void addEntry(TiesEntryExtended entry) throws TiesServiceScopeException {
            total = total.add(SESSION_ENTRY_FEE);
            try {
                processCheques(new ChequeMeta() {

                    @Override
                    public String getTablespaceName() {
                        return entry.getTablespaceName();
                    }

                    @Override
                    public String getTableName() {
                        return entry.getTableName();
                    }

                    @Override
                    public BigInteger getChequeNetwork() {
                        return BigInteger.valueOf(entry.getHeader().getEntryNetwork());
                    }

                    @Override
                    public BigInteger getChequeCropDelta() {
                        return SESSION_ENTRY_FEE;
                    }

                    @Override
                    public byte[] getSigner() {
                        return entry.getHeader().getSigner();
                    }

                }, entry.getCheques()).collect(Collectors.toCollection(() -> this.paidCheques));
            } catch (Throwable th) {
                throw new TiesServiceScopeException("Failed to add Entry to ServiceScope: " + entry.toString(), th);
            }
        }

        public synchronized void addQuery(Query query) throws TiesServiceScopeException {
            total = total.add(SESSION_QUERY_FEE);
            try {
                processCheques(new ChequeMeta() {

                    @Override
                    public String getTablespaceName() {
                        return query.getTablespaceName();
                    }

                    @Override
                    public String getTableName() {
                        return query.getTableName();
                    }

                    @Override
                    public BigInteger getChequeCropDelta() {
                        return SESSION_QUERY_FEE;
                    }

                    @Override
                    public BigInteger getChequeNetwork() {
                        return BigInteger.valueOf(schema.getSchemaNetwork());
                    }

                    @Override
                    public byte[] getSigner() {
                        throw new IllegalStateException("Query Cheques should explicitly include the Signer Address");
                    }

                }, query.getCheques()).collect(Collectors.toCollection(() -> this.paidCheques));
            } catch (Throwable th) {
                throw new TiesServiceScopeException("Failed to add Query to ServiceScope: " + query.toString(), th);
            }
        }

        public void aquire() throws TiesServiceScopeException {
            try {
                this.paidCheques.parallelStream().forEach(ch -> {
                    boolean isAquired = false;
                    TiesServiceScopeException e = null;
                    if (!isAquired) {
                        try {
                            TiesSchemaUtil.updateChequeSession( //
                                    ch.getTablespaceName(), //
                                    ch.getTableName(), //
                                    ch.getChequeSession(), //
                                    ch.getChequeNumber(), //
                                    ch.getChequeCropAmount(), //
                                    ch.getChequeCropDelta(), //
                                    ByteBuffer.wrap(ch.getSigner()), //
                                    ByteBuffer.wrap(ch.getSignature()) //
                            );
                            isAquired = true;
                        } catch (TiesServiceScopeException ex) {
                            e = ex;
                            LOG.trace("Failed to update cheque {}", ch, ex);
                        }
                    }
                    if (!isAquired) {
                        if (0 < SESSION_CREATE_FEE.compareTo(ch.getChequeCropAmount().subtract(ch.getChequeCropDelta()))) {
                            throw new ChequeAquiringException( //
                                    ch, "Crops amount is insufficient to create a new session for Cheque " + printCheque(ch));
                        }
                        try {
                            TiesSchemaUtil.createChequeSession( //
                                    ch.getChequeVersion(), ch.getTablespaceName(), //
                                    ch.getTableName(), //
                                    ch.getChequeSession(), //
                                    ch.getChequeNumber(), //
                                    ch.getChequeCropAmount(), //
                                    ByteBuffer.wrap(ch.getSigner()), //
                                    ByteBuffer.wrap(ch.getSignature()) //
                            );
                            isAquired = true;
                        } catch (TiesServiceScopeException ex) {
                            if (null == e) {
                                e = ex;
                            } else {
                                e.addSuppressed(ex);
                            }
                            LOG.trace("Failed to update cheque {}", ch, ex);
                        }
                    }
                    if (!isAquired) {
                        LOG.error("Failed to aquire cheque {}", printCheque(ch), e);
                    }
                });
            } catch (ChequeAquiringException ex) {
                TiesCheque ch = ex.getCheque();
                throw new TiesServiceScopeException("Cheque not aquired " + ch.getChequeSession() + ":" + ch.getChequeNumber(), ex);
            }
        }

    }

    interface PaidAction extends TiesServiceScopeAction {
        Billing getBilling();
    }

    public TiesServiceScopeBilling(TiesSchema schema) throws TiesConfigurationException {
        this.schema = schema;
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

    public <T extends PaidAction> T aquireActionBilling(T action) throws TiesServiceScopeException {
        getBillingSafe(action).aquire();
        return action;
    }

    private Stream<? extends BillingCheque> processCheques(ChequeMeta meta, Collection<? extends TiesCheque> cheques) {
        return cheques.parallelStream() //
                .map(ch -> new BillingCheque(ch, meta)) //
                .filter(ch -> {
                    try {
                        return checkCheque(ch);
                    } catch (TiesServiceScopeException ex) {
                        LOG.warn("Failed to validate cheque: {}.{}", ch.getChequeSession(), ch.getChequeNumber(), ex);
                        return false;
                    }
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

    public boolean checkCheque(BillingCheque ch) throws TiesServiceScopeException {
        BigInteger network = ch.getChequeNetwork();
        if (this.schema.getSchemaNetwork() != network.shortValueExact())
            return false;
        try {
            return this.schema.isChequeValid(ch);
        } catch (SignatureException ex) {
            throw new TiesServiceScopeException("Cheque validation failed for: " + printCheque(ch) + ".", ex);
        }
    }

    protected static String printCheque(TiesCheque cheque) {
        byte[] signer = cheque.getSigner();
        byte[] signature = cheque.getSignature();
        return "TiesCheque [tablespaceName=" + cheque.getTablespaceName() //
                + ", tableName=" + cheque.getTableName() //
                + ", signer=" + (null == signer ? "null" : new BigInteger(1, signer).toString(16)) //
                + ", chequeSession=" + cheque.getChequeSession() //
                + ", chequeNumber=" + cheque.getChequeNumber() //
                + ", chequeCropAmount=" + cheque.getChequeCropAmount() //
                + ", signature=" + (null == signature ? "null" : new BigInteger(1, signature).toString(16)) //
                + ", chequeNetwork=" + cheque.getChequeNetwork() //
                + ", chequeVersion=" + cheque.getChequeVersion() //
                + "]";
    }

}
