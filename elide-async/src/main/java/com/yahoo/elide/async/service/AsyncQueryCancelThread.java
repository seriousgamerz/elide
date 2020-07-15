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

    try {
        TransactionRegistry transactionRegistry = elide.getTransactionRegistry();
        //System.out.println(transactionRegistry.toString());
        Collection<AsyncQuery> asyncQueryList = asyncQueryDao.getAsyncQueryAndResultCollection();
        for (AsyncQuery obj : asyncQueryList) {
            System.out.println(System.currentTimeMillis() + " ******** Async " + obj.getId()
                    + " *** " + obj.getRequestId());
            UUID uuid = UUID.nameUUIDFromBytes(obj.getRequestId().getBytes());
            System.out.println(System.currentTimeMillis() + " **** running transaction "
                    + transactionRegistry.getRunningTransaction(uuid));

            Map<UUID, DataStoreTransaction> runningTransactionMap = transactionRegistry.getTransactionMap();

            System.out.println(System.currentTimeMillis() + "runningTransactionMap " + runningTransactionMap);

            for (Map.Entry<UUID, DataStoreTransaction> entry : runningTransactionMap.entrySet()) {
                System.out.println(System.currentTimeMillis() + " ******** To be Cancelled" + obj.getId());
                System.out.println(obj.getRequestId().trim());
                System.out.println(entry.getKey().toString().trim());
                if (obj.getRequestId().trim().equals(entry.getKey().toString().trim())) {
                    Date currentDate = new Date(System.currentTimeMillis());
                    long diffInMillies = Math.abs(obj.getUpdatedOn().getTime() - currentDate.getTime());
                    long diffInSecs = TimeUnit.SECONDS.convert(diffInMillies, TimeUnit.MILLISECONDS);
                    if (obj.getStatus().equals(QueryStatus.CANCELLED)) {
                        System.out.println(System.currentTimeMillis() + " ******** Cancelled " + obj.getId());
                        entry.getValue().cancel();
                    } else if (diffInSecs >= maxRunTimeSeconds) {
                        System.out.println(System.currentTimeMillis() + " ******** Cancelled " + obj.getId()
                        + entry.getValue());
                        entry.getValue().cancel();
                        transactionRegistry.removeRunningTransaction(entry.getKey());
                    }
                }
            }
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
    }
}
