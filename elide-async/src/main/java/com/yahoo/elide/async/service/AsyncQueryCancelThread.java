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

import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Runnable thread for cancelling AsyncQuery transactions
 * beyond the max run time or if it has status CANCELLED.
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
     */
    protected void cancelAsyncQuery() {

        try {

            TransactionRegistry transactionRegistry = elide.getTransactionRegistry();
            Map<UUID, DataStoreTransaction> runningTransactionMap = transactionRegistry.getRunningTransactions();

            String filterDateFormatted = evaluateFormattedFilterDate(Calendar.SECOND, maxRunTimeSeconds);
            String filterExpression = "status=in=(" + QueryStatus.CANCELLED.toString() + ","
                    + QueryStatus.PROCESSING.toString() + ","
                    + QueryStatus.QUEUED.toString() + ");createdOn=ge='" + filterDateFormatted + "'";

            Collection<AsyncQuery> asyncQueryCollection = asyncQueryDao.getActiveAsyncQueryCollection(filterExpression);

            Set<UUID> runningTransactions = runningTransactionMap.keySet();

            Set<UUID> asyncTransactions = asyncQueryCollection.stream()
                    .filter(query -> query.getStatus() == QueryStatus.CANCELLED
                    || TimeUnit.SECONDS.convert(Math.abs(query.getUpdatedOn().getTime()
                    - new Date(System.currentTimeMillis()).getTime()), TimeUnit.MILLISECONDS) > maxRunTimeSeconds)
                    .map(AsyncQuery::getRequestId)
            .collect(Collectors.toSet());

            Set<UUID> queriesToCancel = Sets.intersection(runningTransactions, asyncTransactions);

            queriesToCancel.stream()
               .forEach((tx) -> {
                   transactionRegistry.getRunningTransaction(tx).cancel();
               });

        } catch (Exception e) {
            log.error("Exception: {}", e);
        }
    }

    /**
     * Evaluates and subtracts the amount based on the calendar unit and amount from current date.
     * @param calendarUnit Enum such as Calendar.SECOND
     * @param amount Amount of days to be subtracted from current time
     * @return formatted filter date
     */
     private String evaluateFormattedFilterDate(int calendarUnit, int amount) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(calendarUnit, - amount);
        Date filterDate = cal.getTime();
        Format dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String filterDateFormatted = dateFormat.format(filterDate);
        log.debug("FilterDateFormatted = {}", filterDateFormatted);
        return filterDateFormatted;
    }
}
