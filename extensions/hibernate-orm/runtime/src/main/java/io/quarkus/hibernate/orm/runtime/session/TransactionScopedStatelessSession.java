package io.quarkus.hibernate.orm.runtime.session;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.ConnectionConsumer;
import jakarta.persistence.ConnectionFunction;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.FindOption;
import jakarta.persistence.LockModeType;
import jakarta.persistence.TransactionRequiredException;
import jakarta.persistence.TypedQueryReference;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaSelect;
import jakarta.persistence.criteria.CriteriaStatement;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;

import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.SessionFactory;
import org.hibernate.SharedSessionBuilder;
import org.hibernate.SharedStatelessSessionBuilder;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.engine.spi.StatelessSessionLazyDelegator;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.jdbc.ReturningWork;
import org.hibernate.jdbc.Work;
import org.hibernate.procedure.ProcedureCall;
import org.hibernate.query.MutationQuery;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.hibernate.query.SelectionQuery;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.hibernate.query.criteria.JpaCriteriaInsert;

import io.quarkus.arc.Arc;
import io.quarkus.hibernate.orm.runtime.HibernateOrmRuntimeConfig;
import io.quarkus.hibernate.orm.runtime.RequestScopedStatelessSessionHolder;
import io.quarkus.runtime.BlockingOperationControl;
import io.quarkus.runtime.BlockingOperationNotAllowedException;

/**
 * A transaction-scoped {@link StatelessSession} proxy that resolves the real session on each method call.
 * <p>
 * Extends Hibernate's {@link StatelessSessionLazyDelegator} so that new methods added to the
 * {@link StatelessSession} interface by Hibernate ORM are automatically delegated without requiring
 * Quarkus changes. Only methods that need Quarkus-specific behavior (IO-thread guard, transaction
 * requirement, or special lifecycle semantics) are overridden here.
 */
public class TransactionScopedStatelessSession extends StatelessSessionLazyDelegator {

    protected static final String TRANSACTION_IS_NOT_ACTIVE = "Transaction is not active, consider adding @Transactional to your method to automatically activate one.";

    private final TransactionManager transactionManager;
    private final TransactionSynchronizationRegistry transactionSynchronizationRegistry;
    private final SessionFactory sessionFactory;
    private final JTAStatelessSessionOpener jtaSessionOpener;
    private final String unitName;
    private final String sessionKey;
    private final boolean requestScopedSessionEnabled;
    private final Instance<RequestScopedStatelessSessionHolder> requestScopedSessions;

    public TransactionScopedStatelessSession(
            TransactionManager transactionManager,
            TransactionSynchronizationRegistry transactionSynchronizationRegistry,
            SessionFactory sessionFactory,
            String unitName,
            boolean requestScopedSessionEnabled,
            Instance<RequestScopedStatelessSessionHolder> requestScopedSessions) {
        this.transactionManager = transactionManager;
        this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
        this.sessionFactory = sessionFactory;
        this.jtaSessionOpener = JTAStatelessSessionOpener.create(sessionFactory);
        this.unitName = unitName;
        this.sessionKey = TransactionScopedStatelessSession.class.getSimpleName() + "-" + unitName;
        this.requestScopedSessionEnabled = requestScopedSessionEnabled;
        this.requestScopedSessions = requestScopedSessions;
    }

    @Override
    public StatelessSession delegate() {
        return acquireSession();
    }

    StatelessSession acquireSession() {
        checkBlocking();
        if (isInTransaction()) {
            StatelessSession session = (StatelessSession) transactionSynchronizationRegistry.getResource(sessionKey);
            if (session != null) {
                return session;
            }
            StatelessSession newSession = jtaSessionOpener.openSession();
            // The session has automatically joined the JTA transaction when it was constructed.
            transactionSynchronizationRegistry.putResource(sessionKey, newSession);
            return newSession;
        } else if (requestScopedSessionEnabled) {
            if (Arc.container().requestContext().isActive()) {
                return requestScopedSessions.get().getOrCreateSession(unitName, sessionFactory);
            } else {
                throw new ContextNotActiveException(
                        "Cannot use the StatelessSession because neither a transaction nor a CDI request context is active."
                                + " Consider adding @Transactional to your method to automatically activate a transaction,"
                                + " or @ActivateRequestContext if you have valid reasons not to use transactions.");
            }
        } else {
            throw new ContextNotActiveException(
                    "Cannot use the StatelessSession because no transaction is active."
                            + " Consider adding @Transactional to your method to automatically activate a transaction,"
                            + " or set '" + HibernateOrmRuntimeConfig.extensionPropertyKey("request-scoped.enabled")
                            + "' to 'true' if you have valid reasons not to use transactions.");
        }
    }

    private void checkBlocking() {
        if (!BlockingOperationControl.isBlockingAllowed()) {
            throw new BlockingOperationNotAllowedException(
                    "You have attempted to perform a blocking operation on a IO thread. This is not allowed, as blocking the IO thread will cause major performance issues with your application. If you want to perform blocking StatelessSession operations make sure you are doing it from a worker thread.");
        }
    }

    private boolean isInTransaction() {
        try {
            switch (transactionManager.getStatus()) {
                case Status.STATUS_ACTIVE:
                case Status.STATUS_COMMITTING:
                case Status.STATUS_MARKED_ROLLBACK:
                case Status.STATUS_PREPARED:
                case Status.STATUS_PREPARING:
                    return true;
                default:
                    return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // -------------------------------------------------------------------------
    // Special lifecycle methods — do NOT delegate to the underlying session
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        throw new IllegalStateException("Not supported for transaction scoped entity managers");
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public Transaction getTransaction() {
        throw new IllegalStateException("Not supported for JTA entity managers");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> type) {
        if (type.isAssignableFrom(StatelessSession.class)) {
            return (T) this;
        }
        checkBlocking();
        return acquireSession().unwrap(type);
    }

    // -------------------------------------------------------------------------
    // Factory accessors — return directly without acquiring a session
    // -------------------------------------------------------------------------

    @Override
    public EntityManagerFactory getEntityManagerFactory() {
        return sessionFactory;
    }

    @Override
    public SessionFactory getFactory() {
        return sessionFactory;
    }

    // -------------------------------------------------------------------------
    // Methods requiring an active transaction
    // -------------------------------------------------------------------------

    @Override
    public void refresh(Object entity) {
        checkBlocking();
        StatelessSession session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(entity);
    }

    @Override
    public void refresh(String entityName, Object object) {
        checkBlocking();
        StatelessSession session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(entityName, object);
    }

    @Override
    public void refresh(Object object, LockMode lockMode) {
        checkBlocking();
        StatelessSession session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(object, lockMode);
    }

    @Override
    public void refresh(String s, Object o, LockMode lockMode) {
        checkBlocking();
        StatelessSession session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(s, o, lockMode);
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode) {
        checkBlocking();
        StatelessSession session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(entity, lockMode);
    }

    @Override
    public void refreshMultiple(List<?> entities) {
        checkBlocking();
        StatelessSession session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refreshMultiple(entities);
    }

    @Override
    public void inTransaction(Consumer<? super Transaction> action) {
        checkBlocking();
        StatelessSession session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.inTransaction(action);
    }

    @Override
    public <R> R fromTransaction(Function<? super Transaction, R> action) {
        checkBlocking();
        StatelessSession session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        return session.fromTransaction(action);
    }

    // -------------------------------------------------------------------------
    // Blocking-checked operations
    // Methods that only need acquireSession() with no extra guards are inherited
    // from StatelessSessionLazyDelegator (see bottom of file for full list).
    // -------------------------------------------------------------------------

    @Override
    public Object insert(Object o) {
        checkBlocking();
        return acquireSession().insert(o);
    }

    @Override
    public Object insert(String s, Object o) {
        checkBlocking();
        return acquireSession().insert(s, o);
    }

    @Override
    public void insertMultiple(List<?> entities) {
        checkBlocking();
        acquireSession().insertMultiple(entities);
    }

    @Deprecated
    @Override
    public void update(Object object) {
        checkBlocking();
        acquireSession().update(object);
    }

    @Deprecated
    @Override
    public void update(String entityName, Object object) {
        checkBlocking();
        acquireSession().update(entityName, object);
    }

    @Override
    public void updateMultiple(List<?> entities) {
        checkBlocking();
        acquireSession().updateMultiple(entities);
    }

    @Deprecated
    @Override
    public void delete(Object object) {
        checkBlocking();
        acquireSession().delete(object);
    }

    @Deprecated
    @Override
    public void delete(String entityName, Object object) {
        checkBlocking();
        acquireSession().delete(entityName, object);
    }

    @Override
    public void deleteMultiple(List<?> entities) {
        checkBlocking();
        acquireSession().deleteMultiple(entities);
    }

    @Override
    public void upsert(Object entity) {
        checkBlocking();
        acquireSession().upsert(entity);
    }

    @Override
    public void upsert(String entityName, Object entity) {
        checkBlocking();
        acquireSession().upsert(entityName, entity);
    }

    @Override
    public void upsertMultiple(List<?> entities) {
        checkBlocking();
        acquireSession().upsertMultiple(entities);
    }

    @Override
    public <T> T fetch(T entity) {
        checkBlocking();
        return acquireSession().fetch(entity);
    }

    @Override
    public Object getIdentifier(Object entity) {
        checkBlocking();
        return acquireSession().getIdentifier(entity);
    }

    @Override
    public <T> T get(Class<T> entityType, Object id) {
        checkBlocking();
        return acquireSession().get(entityType, id);
    }

    @Override
    public <T> T get(Class<T> entityType, Object id, LockMode lockMode) {
        checkBlocking();
        return acquireSession().get(entityType, id, lockMode);
    }

    @Override
    public <T> T get(Class<T> entityType, Object id, FindOption... options) {
        checkBlocking();
        return acquireSession().get(entityType, id, options);
    }

    @Override
    public Object get(String entityName, Object id) {
        checkBlocking();
        return acquireSession().get(entityName, id);
    }

    @Override
    public Object get(String entityName, Object id, LockMode lockMode) {
        checkBlocking();
        return acquireSession().get(entityName, id, lockMode);
    }

    @Override
    public Object get(String entityName, Object id, FindOption... options) {
        checkBlocking();
        return acquireSession().get(entityName, id, options);
    }

    @Override
    public <T> T get(EntityGraph<T> graph, Object id) {
        checkBlocking();
        return acquireSession().get(graph, id);
    }

    @Override
    public <T> T get(EntityGraph<T> graph, Object id, LockMode lockMode) {
        checkBlocking();
        return acquireSession().get(graph, id, lockMode);
    }

    @Override
    public <T> T get(EntityGraph<T> graph, GraphSemantic graphSemantic, Object id) {
        checkBlocking();
        return acquireSession().get(graph, graphSemantic, id);
    }

    @Override
    public <T> T get(EntityGraph<T> graph, GraphSemantic graphSemantic, Object id, LockMode lockMode) {
        checkBlocking();
        return acquireSession().get(graph, graphSemantic, id, lockMode);
    }

    @Override
    public <T> T get(EntityGraph<T> entityGraph, Object id, FindOption... options) {
        checkBlocking();
        return acquireSession().get(entityGraph, id, options);
    }

    @Override
    public <T> List<T> getMultiple(Class<T> entityClass, List<?> ids) {
        checkBlocking();
        return acquireSession().getMultiple(entityClass, ids);
    }

    @Override
    public <T> List<T> getMultiple(Class<T> entityClass, List<?> ids, LockMode lockMode) {
        checkBlocking();
        return acquireSession().getMultiple(entityClass, ids, lockMode);
    }

    @Override
    public <T> List<T> getMultiple(Class<T> entityClass, List<?> ids, FindOption... options) {
        checkBlocking();
        return acquireSession().getMultiple(entityClass, ids, options);
    }

    @Override
    public <T> List<T> getMultiple(EntityGraph<T> entityGraph, List<?> ids) {
        checkBlocking();
        return acquireSession().getMultiple(entityGraph, ids);
    }

    @Override
    public <T> List<T> getMultiple(EntityGraph<T> entityGraph, GraphSemantic graphSemantic, List<?> ids) {
        checkBlocking();
        return acquireSession().getMultiple(entityGraph, graphSemantic, ids);
    }

    @Override
    public <T> List<T> getMultiple(EntityGraph<T> entityGraph, List<?> ids, FindOption... options) {
        checkBlocking();
        return acquireSession().getMultiple(entityGraph, ids, options);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object id) {
        checkBlocking();
        return acquireSession().find(entityClass, id);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object id, FindOption... options) {
        checkBlocking();
        return acquireSession().find(entityClass, id, options);
    }

    @Override
    public <T> T find(EntityGraph<T> entityGraph, Object id, FindOption... options) {
        checkBlocking();
        return acquireSession().find(entityGraph, id, options);
    }

    @Override
    public Object find(String entityName, Object id, FindOption... options) {
        checkBlocking();
        return acquireSession().find(entityName, id, options);
    }

    @Override
    public <E> List<E> findMultiple(Class<E> entityType, List<?> ids, FindOption... options) {
        checkBlocking();
        return acquireSession().findMultiple(entityType, ids, options);
    }

    @Override
    public <E> List<E> findMultiple(EntityGraph<E> entityGraph, List<?> ids, FindOption... options) {
        checkBlocking();
        return acquireSession().findMultiple(entityGraph, ids, options);
    }

    @Override
    public Filter enableFilter(String filterName) {
        checkBlocking();
        return acquireSession().enableFilter(filterName);
    }

    @Override
    public Filter getEnabledFilter(String filterName) {
        checkBlocking();
        return acquireSession().getEnabledFilter(filterName);
    }

    @Override
    public void disableFilter(String filterName) {
        checkBlocking();
        acquireSession().disableFilter(filterName);
    }

    @Override
    public boolean isConnected() {
        checkBlocking();
        return acquireSession().isConnected();
    }

    @Override
    public Transaction beginTransaction() {
        checkBlocking();
        return acquireSession().beginTransaction();
    }

    @Deprecated
    @Override
    public Query createQuery(String qlString) {
        checkBlocking();
        return acquireSession().createQuery(qlString);
    }

    @Override
    public <T> Query<T> createQuery(CriteriaQuery<T> criteriaQuery) {
        checkBlocking();
        return acquireSession().createQuery(criteriaQuery);
    }

    @Deprecated
    @Override
    public Query createQuery(CriteriaUpdate updateQuery) {
        checkBlocking();
        return acquireSession().createQuery(updateQuery);
    }

    @Deprecated
    @Override
    public Query createQuery(CriteriaDelete deleteQuery) {
        checkBlocking();
        return acquireSession().createQuery(deleteQuery);
    }

    @Override
    public <T> Query<T> createQuery(String qlString, Class<T> resultClass) {
        checkBlocking();
        return acquireSession().createQuery(qlString, resultClass);
    }

    @Override
    public <R> Query<R> createQuery(TypedQueryReference<R> typedQueryReference) {
        checkBlocking();
        return acquireSession().createQuery(typedQueryReference);
    }

    @Deprecated
    @Override
    public Query createNamedQuery(String name) {
        checkBlocking();
        return acquireSession().createNamedQuery(name);
    }

    @Override
    public <T> Query<T> createNamedQuery(String name, Class<T> resultClass) {
        checkBlocking();
        return acquireSession().createNamedQuery(name, resultClass);
    }

    @Deprecated
    @Override
    public NativeQuery createNativeQuery(String sqlString) {
        checkBlocking();
        return acquireSession().createNativeQuery(sqlString);
    }

    @Deprecated
    @Override
    public NativeQuery createNativeQuery(String sqlString, Class resultClass) {
        checkBlocking();
        return acquireSession().createNativeQuery(sqlString, resultClass);
    }

    @Deprecated
    @Override
    public NativeQuery createNativeQuery(String sqlString, String resultSetMapping) {
        checkBlocking();
        return acquireSession().createNativeQuery(sqlString, resultSetMapping);
    }

    @Override
    public <R> NativeQuery<R> createNativeQuery(String sqlString, Class<R> resultClass, String tableAlias) {
        checkBlocking();
        return acquireSession().createNativeQuery(sqlString, resultClass, tableAlias);
    }

    @Override
    public <R> NativeQuery<R> createNativeQuery(String sqlString, String resultSetMappingName, Class<R> resultClass) {
        checkBlocking();
        return acquireSession().createNativeQuery(sqlString, resultSetMappingName, resultClass);
    }

    @Override
    public ProcedureCall createNamedStoredProcedureQuery(String name) {
        checkBlocking();
        return acquireSession().createNamedStoredProcedureQuery(name);
    }

    @Override
    public ProcedureCall createStoredProcedureQuery(String procedureName) {
        checkBlocking();
        return acquireSession().createStoredProcedureQuery(procedureName);
    }

    @Override
    public ProcedureCall createStoredProcedureQuery(String procedureName,
            @SuppressWarnings("rawtypes") Class... resultClasses) {
        checkBlocking();
        return acquireSession().createStoredProcedureQuery(procedureName, resultClasses);
    }

    @Override
    public ProcedureCall createStoredProcedureQuery(String procedureName, String... resultSetMappings) {
        checkBlocking();
        return acquireSession().createStoredProcedureQuery(procedureName, resultSetMappings);
    }

    @Override
    public HibernateCriteriaBuilder getCriteriaBuilder() {
        checkBlocking();
        return acquireSession().getCriteriaBuilder();
    }

    @Deprecated
    @Override
    public Query getNamedQuery(String queryName) {
        checkBlocking();
        return acquireSession().getNamedQuery(queryName);
    }

    @Override
    public ProcedureCall getNamedProcedureCall(String name) {
        checkBlocking();
        return acquireSession().getNamedProcedureCall(name);
    }

    @Override
    public ProcedureCall createStoredProcedureCall(String procedureName) {
        checkBlocking();
        return acquireSession().createStoredProcedureCall(procedureName);
    }

    @Override
    public ProcedureCall createStoredProcedureCall(String procedureName, Class... resultClasses) {
        checkBlocking();
        return acquireSession().createStoredProcedureCall(procedureName, resultClasses);
    }

    @Override
    public ProcedureCall createStoredProcedureCall(String procedureName, String... resultSetMappings) {
        checkBlocking();
        return acquireSession().createStoredProcedureCall(procedureName, resultSetMappings);
    }

    @Override
    public void doWork(Work work) throws HibernateException {
        checkBlocking();
        acquireSession().doWork(work);
    }

    @Override
    public <T> T doReturningWork(ReturningWork<T> work) throws HibernateException {
        checkBlocking();
        return acquireSession().doReturningWork(work);
    }

    @Override
    public <C> void runWithConnection(ConnectionConsumer<C> action) {
        checkBlocking();
        acquireSession().runWithConnection(action);
    }

    @Override
    public <C, T> T callWithConnection(ConnectionFunction<C, T> function) {
        checkBlocking();
        return acquireSession().callWithConnection(function);
    }

    @Deprecated
    @Override
    public NativeQuery getNamedNativeQuery(String name) {
        checkBlocking();
        return acquireSession().getNamedNativeQuery(name);
    }

    @Deprecated
    @Override
    public NativeQuery getNamedNativeQuery(String name, String resultSetMapping) {
        checkBlocking();
        return acquireSession().getNamedNativeQuery(name, resultSetMapping);
    }

    @Override
    public <T> RootGraph<T> createEntityGraph(Class<T> rootType) {
        checkBlocking();
        return acquireSession().createEntityGraph(rootType);
    }

    @Override
    public RootGraph<?> createEntityGraph(String graphName) {
        checkBlocking();
        return acquireSession().createEntityGraph(graphName);
    }

    @Override
    public <T> RootGraph<T> createEntityGraph(Class<T> rootType, String graphName) {
        checkBlocking();
        return acquireSession().createEntityGraph(rootType, graphName);
    }

    @Override
    public RootGraph<?> getEntityGraph(String graphName) {
        checkBlocking();
        return acquireSession().getEntityGraph(graphName);
    }

    @Override
    public <T> RootGraph<T> getEntityGraph(Class<T> rootType, String graphName) {
        checkBlocking();
        return acquireSession().getEntityGraph(rootType, graphName);
    }

    @Override
    public <T> List<EntityGraph<? super T>> getEntityGraphs(Class<T> entityClass) {
        checkBlocking();
        return acquireSession().getEntityGraphs(entityClass);
    }

    @Override
    public SharedSessionBuilder sessionWithOptions() {
        checkBlocking();
        return acquireSession().sessionWithOptions();
    }

    @Override
    public SharedStatelessSessionBuilder statelessWithOptions() {
        checkBlocking();
        return acquireSession().statelessWithOptions();
    }

    @Override
    public SelectionQuery<?> createSelectionQuery(String hqlString) {
        checkBlocking();
        return acquireSession().createSelectionQuery(hqlString);
    }

    @Override
    public <R> SelectionQuery<R> createSelectionQuery(String hqlString, Class<R> resultType) {
        checkBlocking();
        return acquireSession().createSelectionQuery(hqlString, resultType);
    }

    @Override
    public <R> SelectionQuery<R> createSelectionQuery(CriteriaSelect<R> criteria) {
        checkBlocking();
        return acquireSession().createSelectionQuery(criteria);
    }

    @Override
    public <R> SelectionQuery<R> createSelectionQuery(String hqlString, EntityGraph<R> resultGraph) {
        checkBlocking();
        return acquireSession().createSelectionQuery(hqlString, resultGraph);
    }

    @Override
    public MutationQuery createMutationQuery(String hqlString) {
        checkBlocking();
        return acquireSession().createMutationQuery(hqlString);
    }

    @Override
    public MutationQuery createMutationQuery(CriteriaUpdate updateQuery) {
        checkBlocking();
        return acquireSession().createMutationQuery(updateQuery);
    }

    @Override
    public MutationQuery createMutationQuery(CriteriaDelete deleteQuery) {
        checkBlocking();
        return acquireSession().createMutationQuery(deleteQuery);
    }

    @Override
    public MutationQuery createMutationQuery(JpaCriteriaInsert insert) {
        checkBlocking();
        return acquireSession().createMutationQuery(insert);
    }

    @Override
    public MutationQuery createMutationQuery(CriteriaStatement<?> criteriaStatement) {
        checkBlocking();
        return acquireSession().createMutationQuery(criteriaStatement);
    }

    @Override
    public MutationQuery createNativeMutationQuery(String sqlString) {
        checkBlocking();
        return acquireSession().createNativeMutationQuery(sqlString);
    }

    @Override
    public SelectionQuery<?> createNamedSelectionQuery(String name) {
        checkBlocking();
        return acquireSession().createNamedSelectionQuery(name);
    }

    @Override
    public <R> SelectionQuery<R> createNamedSelectionQuery(String name, Class<R> resultType) {
        checkBlocking();
        return acquireSession().createNamedSelectionQuery(name, resultType);
    }

    @Override
    public MutationQuery createNamedMutationQuery(String name) {
        checkBlocking();
        return acquireSession().createNamedMutationQuery(name);
    }

    @Override
    public CacheMode getCacheMode() {
        checkBlocking();
        return acquireSession().getCacheMode();
    }

    @Override
    public void setCacheMode(CacheMode cacheMode) {
        checkBlocking();
        acquireSession().setCacheMode(cacheMode);
    }

    @Override
    public CacheStoreMode getCacheStoreMode() {
        checkBlocking();
        return acquireSession().getCacheStoreMode();
    }

    @Override
    public void setCacheStoreMode(CacheStoreMode cacheStoreMode) {
        checkBlocking();
        acquireSession().setCacheStoreMode(cacheStoreMode);
    }

    @Override
    public CacheRetrieveMode getCacheRetrieveMode() {
        checkBlocking();
        return acquireSession().getCacheRetrieveMode();
    }

    @Override
    public void setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
        checkBlocking();
        acquireSession().setCacheRetrieveMode(cacheRetrieveMode);
    }

    // -------------------------------------------------------------------------
    // The following methods are fully inherited from StatelessSessionLazyDelegator
    // because neither a blocking check nor a transaction requirement exists for them:
    //   getTenantIdentifier / getTenantIdentifierValue
    //   getJdbcBatchSize / setJdbcBatchSize
    //   joinTransaction / isJoinedToTransaction
    //   getProperties / setProperty
    //   getMetamodel
    //   addOption / getOptions (EntityAgent options, if present in Hibernate 8)
    // -------------------------------------------------------------------------
}
