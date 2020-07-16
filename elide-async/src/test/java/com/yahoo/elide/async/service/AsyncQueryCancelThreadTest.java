/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.async.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.yahoo.elide.Elide;
import com.yahoo.elide.ElideSettingsBuilder;
import com.yahoo.elide.async.models.AsyncQuery;
import com.yahoo.elide.core.EntityDictionary;
import com.yahoo.elide.core.TransactionRegistry;
import com.yahoo.elide.core.datastore.inmemory.HashMapDataStore;
import com.yahoo.elide.security.checks.Check;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class AsyncQueryCancelThreadTest {

    private AsyncQueryCancelThread cancelThread;
    private Elide elide;
    private AsyncQueryDAO asyncQueryDao;
    private TransactionRegistry transactionRegistry;

    @BeforeEach
    public void setupMocks() {
        HashMapDataStore inMemoryStore = new HashMapDataStore(AsyncQuery.class.getPackage());
        Map<String, Class<? extends Check>> checkMappings = new HashMap<>();

        elide = new Elide(
                new ElideSettingsBuilder(inMemoryStore)
                        .withEntityDictionary(new EntityDictionary(checkMappings))
                        .withISO8601Dates("yyyy-MM-dd'T'HH:mm'Z'", TimeZone.getTimeZone("UTC"))
                        .build());

        asyncQueryDao = mock(DefaultAsyncQueryDAO.class);
        transactionRegistry = mock(TransactionRegistry.class);
        cancelThread = new AsyncQueryCancelThread(7, elide, asyncQueryDao, transactionRegistry);
    }

    @Test
    public void testAsyncQueryCancelThreadSet() {
        assertEquals(elide, cancelThread.getElide());
        assertEquals(asyncQueryDao, cancelThread.getAsyncQueryDao());
        assertEquals(transactionRegistry, cancelThread.getTransactionRegistry());
        assertEquals(7, cancelThread.getMaxRunTimeSeconds());
    }

    @Test
    public void testCancelAsyncQuery() {
        cancelThread.cancelAsyncQuery();
        verify(asyncQueryDao, times(1)).getActiveAsyncQueryCollection();
        verify(transactionRegistry, times(1)).getTransactionMap();
    }
}
