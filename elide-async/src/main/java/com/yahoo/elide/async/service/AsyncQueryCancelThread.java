/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.async.service;

import com.yahoo.elide.Elide;
import com.yahoo.elide.async.models.AsyncQuery;
import com.yahoo.elide.async.models.QueryStatus;
import com.yahoo.elide.core.DataStoreTransaction;
import com.yahoo.elide.core.TransactionRegistry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Runnable thread for cancelling AsyncQuery Transaction
 * beyond the max run time and if it has status CANCELLED.
 */
@Slf4j
@Data
@AllArgsConstructor
public class AsyncQueryCancelThread implements Runnable {

    private int maxRunTimeSeconds;
    private Elide elide;
    private AsyncQueryDAO asyncQueryDao;

    @Override
    public void run() {
        cancelAsyncQuery();
    }

    /**
     * This method cancels queries based on threshold.
     * */
    protected void cancelAsyncQuery() {

        TransactionRegistry transactionRegistry = elide.getTransactionRegistry();
        Map<UUID, DataStoreTransaction> runningTransactionMap = transactionRegistry.getTransactionMap();
        Collection<AsyncQuery> asyncQueryList = asyncQueryDao.getAsyncQueryAndResultCollection();
        for (AsyncQuery obj : asyncQueryList) {
            for (Map.Entry<UUID, DataStoreTransaction> entry : runningTransactionMap.entrySet()) {
                if (obj.getRequestId().trim().equals(entry.getKey().toString().trim())) {
                    Date currentDate = new Date(System.currentTimeMillis());
                    long diffInMillies = Math.abs(obj.getUpdatedOn().getTime() - currentDate.getTime());
                    long diffInSecs = TimeUnit.SECONDS.convert(diffInMillies, TimeUnit.MILLISECONDS);
                    if (obj.getStatus().equals(QueryStatus.CANCELLED)) {
                        entry.getValue().cancel();
                    } else if (diffInSecs >= maxRunTimeSeconds) {
                        entry.getValue().cancel();
                        transactionRegistry.removeRunningTransaction(entry.getKey());
                    }
                }
            }
        }
    }
}
