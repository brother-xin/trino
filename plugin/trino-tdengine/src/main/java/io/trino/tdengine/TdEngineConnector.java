package io.trino.tdengine;

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static io.trino.tdengine.TdEngineTransactionHandle.INSTANCE;

/**
 * @see https://trino.io/docs/current/develop/connectors.html
 */
public class TdEngineConnector implements Connector {
    private final LifeCycleManager lifeCycleManager;
    private final TdEngineConnectorMetadata metadata;
    private final TdEngineSplitManager splitManager;
    private final TdEngineRecordSetProvider recordSetProvider;

    @Inject
    public TdEngineConnector(LifeCycleManager lifeCycleManager, TdEngineConnectorMetadata metadata, TdEngineSplitManager splitManager, TdEngineRecordSetProvider recordSetProvider) {
        this.lifeCycleManager = lifeCycleManager;
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.recordSetProvider = recordSetProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public final void shutdown() {
        lifeCycleManager.stop();
    }
}
