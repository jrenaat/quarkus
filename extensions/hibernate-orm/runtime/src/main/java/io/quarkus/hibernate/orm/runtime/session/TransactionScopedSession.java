package io.quarkus.hibernate.orm.runtime.session;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.persistence.ConnectionConsumer;
import jakarta.persistence.ConnectionFunction;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.FindOption;
import jakarta.persistence.LockModeType;
import jakarta.persistence.LockOption;
import jakarta.persistence.RefreshOption;
import jakarta.persistence.TransactionRequiredException;
import jakarta.persistence.TypedQueryReference;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaSelect;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;

import org.hibernate.Filter;
import org.hibernate.HibernateException;
import org.hibernate.IdentifierLoadAccess;
import org.hibernate.LobHelper;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MultiIdentifierLoadAccess;
import org.hibernate.NaturalIdLoadAccess;
import org.hibernate.NaturalIdMultiLoadAccess;
import org.hibernate.ReplicationMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.SharedSessionBuilder;
import org.hibernate.SharedStatelessSessionBuilder;
import org.hibernate.SimpleNaturalIdLoadAccess;
import org.hibernate.Transaction;
import org.hibernate.engine.spi.SessionLazyDelegator;
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
import io.quarkus.hibernate.orm.runtime.RequestScopedSessionHolder;
import io.quarkus.runtime.BlockingOperationControl;
import io.quarkus.runtime.BlockingOperationNotAllowedException;

/**
 * A transaction-scoped {@link Session} proxy that resolves the real session on each method call.
 * <p>
 * Extends Hibernate's {@link SessionLazyDelegator} so that new methods added to the {@link Session}
 * interface by Hibernate ORM are automatically delegated without requiring Quarkus changes.
 * Only methods that need Quarkus-specific behavior (IO-thread guard, transaction requirement,
 * or special lifecycle semantics) are overridden here.
 * <p>
 * Note: {@link TransactionScopedStatelessSession} cannot yet use the same pattern because
 * Hibernate ORM does not yet provide an equivalent {@code StatelessSessionLazyDelegator}.
 */
public class TransactionScopedSession extends SessionLazyDelegator {

    protected static final String TRANSACTION_IS_NOT_ACTIVE = "Transaction is not active, consider adding @Transactional to your method to automatically activate one.";

    private final TransactionManager transactionManager;
    private final TransactionSynchronizationRegistry transactionSynchronizationRegistry;
    private final SessionFactory sessionFactory;
    private final JTASessionOpener jtaSessionOpener;
    private final String unitName;
    private final String sessionKey;
    private final boolean requestScopedSessionEnabled;
    private final Instance<RequestScopedSessionHolder> requestScopedSessions;

    // Private constructor that uses a one-element array to safely capture `this` in the
    // supplier passed to SessionLazyDelegator before `this` is fully constructed.
    // holder[0] is set to `this` after super() returns; the supplier is never called
    // during construction, so holder[0] is always non-null when the supplier is first invoked.
    private TransactionScopedSession(
            TransactionManager transactionManager,
            TransactionSynchronizationRegistry transactionSynchronizationRegistry,
            SessionFactory sessionFactory,
            String unitName,
            boolean requestScopedSessionEnabled,
            Instance<RequestScopedSessionHolder> requestScopedSessions,
            TransactionScopedSession[] holder) {
        super(() -> holder[0].acquireSession());
        this.transactionManager = transactionManager;
        this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
        this.sessionFactory = sessionFactory;
        this.jtaSessionOpener = JTASessionOpener.create(sessionFactory);
        this.unitName = unitName;
        this.sessionKey = TransactionScopedSession.class.getSimpleName() + "-" + unitName;
        this.requestScopedSessionEnabled = requestScopedSessionEnabled;
        this.requestScopedSessions = requestScopedSessions;
        holder[0] = this;
    }

    public TransactionScopedSession(
            TransactionManager transactionManager,
            TransactionSynchronizationRegistry transactionSynchronizationRegistry,
            SessionFactory sessionFactory,
            String unitName,
            boolean requestScopedSessionEnabled,
            Instance<RequestScopedSessionHolder> requestScopedSessions) {
        this(transactionManager, transactionSynchronizationRegistry, sessionFactory, unitName,
                requestScopedSessionEnabled, requestScopedSessions, new TransactionScopedSession[1]);
    }

    Session acquireSession() {
        if (isInTransaction()) {
            Session session = (Session) transactionSynchronizationRegistry.getResource(sessionKey);
            if (session != null) {
                return session;
            }
            Session newSession = jtaSessionOpener.openSession();
            // The session has automatically joined the JTA transaction when it was constructed.
            transactionSynchronizationRegistry.putResource(sessionKey, newSession);
            // No need to flush or close the session upon transaction completion:
            // Hibernate ORM itself registers a synchronization that does just that.
            // See:
            // - io.quarkus.hibernate.orm.runtime.boot.FastBootMetadataBuilder.mergeSettings
            // - org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorImpl.joinJtaTransaction
            // - org.hibernate.internal.SessionImpl.beforeTransactionCompletion
            // - org.hibernate.internal.SessionImpl.afterTransactionCompletion
            return newSession;
        } else if (requestScopedSessionEnabled) {
            if (Arc.container().requestContext().isActive()) {
                return requestScopedSessions.get().getOrCreateSession(unitName, sessionFactory);
            } else {
                throw new ContextNotActiveException(
                        "Cannot use the EntityManager/Session because neither a transaction nor a CDI request context is active."
                                + " Consider adding @Transactional to your method to automatically activate a transaction,"
                                + " or @ActivateRequestContext if you have valid reasons not to use transactions.");
            }
        } else {
            throw new ContextNotActiveException(
                    "Cannot use the EntityManager/Session because no transaction is active."
                            + " Consider adding @Transactional to your method to automatically activate a transaction,"
                            + " or set '" + HibernateOrmRuntimeConfig.extensionPropertyKey("request-scoped.enabled")
                            + "' to 'true' if you have valid reasons not to use transactions.");
        }
    }

    private void checkBlocking() {
        if (!BlockingOperationControl.isBlockingAllowed()) {
            throw new BlockingOperationNotAllowedException(
                    "You have attempted to perform a blocking operation on a IO thread. This is not allowed, as blocking the IO thread will cause major performance issues with your application. If you want to perform blocking EntityManager operations make sure you are doing it from a worker thread.");
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
    public <T> T unwrap(Class<T> cls) {
        if (cls.isAssignableFrom(Session.class)) {
            return (T) this;
        }
        checkBlocking();
        return acquireSession().unwrap(cls);
    }

    // -------------------------------------------------------------------------
    // Factory accessors — return directly without acquiring a session
    // -------------------------------------------------------------------------

    @Override
    public EntityManagerFactory getEntityManagerFactory() {
        return sessionFactory;
    }

    @Override
    public SessionFactory getSessionFactory() {
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
    public void flush() {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.flush();
    }

    @Override
    public void inTransaction(Consumer<? super Transaction> action) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.inTransaction(action);
    }

    @Override
    public <R> R fromTransaction(Function<? super Transaction, R> action) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        return session.fromTransaction(action);
    }

    // -------------------------------------------------------------------------
    // Blocking-checked operations
    // Methods that only need acquireSession() with no extra guards are inherited
    // from SessionLazyDelegator (see bottom of file for full list).
    // -------------------------------------------------------------------------

    @Override
    public void persist(Object entity) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.persist(entity);
    }

    @Override
    public void persist(String entityName, Object object) {
        checkBlocking();
        acquireSession().persist(entityName, object);
    }

    @Override
    public <T> T merge(T entity) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        return session.merge(entity);
    }

    @Override
    public <T> T merge(String entityName, T object) {
        checkBlocking();
        return acquireSession().merge(entityName, object);
    }

    @Override
    public <T> T merge(T object, EntityGraph<? super T> loadGraph) {
        checkBlocking();
        return acquireSession().merge(object, loadGraph);
    }

    @Override
    public void remove(Object entity) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.remove(entity);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey) {
        checkBlocking();
        return acquireSession().find(entityClass, primaryKey);
    }

    @Override
    public Object find(String entityName, Object primaryKey) {
        checkBlocking();
        return acquireSession().find(entityName, primaryKey);
    }

    @Override
    public Object find(String entityName, Object primaryKey, FindOption... options) {
        checkBlocking();
        return acquireSession().find(entityName, primaryKey, options);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
        checkBlocking();
        return acquireSession().find(entityClass, primaryKey, properties);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode) {
        checkBlocking();
        return acquireSession().find(entityClass, primaryKey, lockMode);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode,
            Map<String, Object> properties) {
        checkBlocking();
        return acquireSession().find(entityClass, primaryKey, lockMode, properties);
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, FindOption... options) {
        checkBlocking();
        return acquireSession().find(entityClass, primaryKey, options);
    }

    @Override
    public <T> T find(EntityGraph<T> entityGraph, Object primaryKey, FindOption... options) {
        checkBlocking();
        return acquireSession().find(entityGraph, primaryKey, options);
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
    public <T> T getReference(Class<T> entityClass, Object primaryKey) {
        checkBlocking();
        return acquireSession().getReference(entityClass, primaryKey);
    }

    @Override
    public Object getReference(String entityName, Object id) {
        checkBlocking();
        return acquireSession().getReference(entityName, id);
    }

    @Override
    public <T> T getReference(T object) {
        checkBlocking();
        return acquireSession().getReference(object);
    }

    @Override
    public LockModeType getLockMode(Object entity) {
        checkBlocking();
        return acquireSession().getLockMode(entity);
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

    @Override
    public <T> jakarta.persistence.TypedQuery<T> createQuery(CriteriaSelect<T> selectQuery) {
        checkBlocking();
        return acquireSession().createQuery(selectQuery);
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
    public void cancelQuery() throws HibernateException {
        checkBlocking();
        acquireSession().cancelQuery();
    }

    @Override
    public boolean isDirty() throws HibernateException {
        checkBlocking();
        return acquireSession().isDirty();
    }

    @Override
    public void load(Object object, Object id) {
        checkBlocking();
        acquireSession().load(object, id);
    }

    @Deprecated
    @Override
    public void replicate(Object object, ReplicationMode replicationMode) {
        checkBlocking();
        acquireSession().replicate(object, replicationMode);
    }

    @Deprecated
    @Override
    public void replicate(String entityName, Object object, ReplicationMode replicationMode) {
        checkBlocking();
        acquireSession().replicate(entityName, object, replicationMode);
    }

    @Override
    public void lock(Object object, LockMode lockMode) {
        checkBlocking();
        acquireSession().lock(object, lockMode);
    }

    @Override
    public void lock(Object entity, LockModeType lockMode) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.lock(entity, lockMode);
    }

    @Override
    public void lock(Object entity, LockModeType lockMode, Map<String, Object> properties) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.lock(entity, lockMode, properties);
    }

    @Override
    public void lock(Object entity, LockModeType lockMode, LockOption... options) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.lock(entity, lockMode, options);
    }

    @Override
    public void refresh(Object entity) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(entity);
    }

    @Override
    public void refresh(Object entity, Map<String, Object> properties) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(entity, properties);
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(entity, lockMode);
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode, Map<String, Object> properties) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(entity, lockMode, properties);
    }

    @Override
    public void refresh(Object entity, RefreshOption... options) {
        checkBlocking();
        Session session = acquireSession();
        if (!isInTransaction()) {
            throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
        }
        session.refresh(entity, options);
    }

    @Override
    public void refresh(Object object, LockOptions lockOptions) {
        checkBlocking();
        acquireSession().refresh(object, lockOptions);
    }

    @Override
    public LockMode getCurrentLockMode(Object object) {
        checkBlocking();
        return acquireSession().getCurrentLockMode(object);
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
    public <T> T get(Class<T> entityType, Object id, LockOptions lockOptions) {
        checkBlocking();
        return acquireSession().get(entityType, id, lockOptions);
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
    public Object get(String entityName, Object id, LockOptions lockOptions) {
        checkBlocking();
        return acquireSession().get(entityName, id, lockOptions);
    }

    @Override
    public <T> IdentifierLoadAccess<T> byId(String entityName) {
        checkBlocking();
        return acquireSession().byId(entityName);
    }

    @Override
    public <T> IdentifierLoadAccess<T> byId(Class<T> entityClass) {
        checkBlocking();
        return acquireSession().byId(entityClass);
    }

    @Override
    public <T> MultiIdentifierLoadAccess<T> byMultipleIds(Class<T> entityClass) {
        checkBlocking();
        return acquireSession().byMultipleIds(entityClass);
    }

    @Override
    public <T> MultiIdentifierLoadAccess<T> byMultipleIds(String entityName) {
        checkBlocking();
        return acquireSession().byMultipleIds(entityName);
    }

    @Override
    public <T> NaturalIdLoadAccess<T> byNaturalId(String entityName) {
        checkBlocking();
        return acquireSession().byNaturalId(entityName);
    }

    @Override
    public <T> NaturalIdLoadAccess<T> byNaturalId(Class<T> entityClass) {
        checkBlocking();
        return acquireSession().byNaturalId(entityClass);
    }

    @Override
    public <T> SimpleNaturalIdLoadAccess<T> bySimpleNaturalId(String entityName) {
        checkBlocking();
        return acquireSession().bySimpleNaturalId(entityName);
    }

    @Override
    public <T> SimpleNaturalIdLoadAccess<T> bySimpleNaturalId(Class<T> entityClass) {
        checkBlocking();
        return acquireSession().bySimpleNaturalId(entityClass);
    }

    @Override
    public <T> NaturalIdMultiLoadAccess<T> byMultipleNaturalId(Class<T> entityClass) {
        checkBlocking();
        return acquireSession().byMultipleNaturalId(entityClass);
    }

    @Override
    public <T> NaturalIdMultiLoadAccess<T> byMultipleNaturalId(String entityName) {
        checkBlocking();
        return acquireSession().byMultipleNaturalId(entityName);
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
    public LobHelper getLobHelper() {
        checkBlocking();
        return acquireSession().getLobHelper();
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
    public <R> SelectionQuery<R> createSelectionQuery(CriteriaQuery<R> criteria) {
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

    // -------------------------------------------------------------------------
    // The following methods are fully inherited from SessionLazyDelegator because
    // neither a blocking check nor a transaction requirement exists for them:
    //   setFlushMode / getFlushMode / setHibernateFlushMode / getHibernateFlushMode
    //   setCacheMode / getCacheMode / setCacheStoreMode / getCacheStoreMode
    //   setCacheRetrieveMode / getCacheRetrieveMode
    //   clear / detach / contains(Object) / setProperty / getProperties
    //   joinTransaction / isJoinedToTransaction / getDelegate / getMetamodel
    //   createEntityGraph / getEntityGraph / getEntityGraphs
    //   isDefaultReadOnly / setDefaultReadOnly
    //   getIdentifier / contains(String, Object) / evict / getEntityName
    //   getStatistics / isReadOnly / setReadOnly
    //   isFetchProfileEnabled / enableFetchProfile / disableFetchProfile
    //   getManagedEntities (all variants) / addEventListeners
    //   getTenantIdentifier / getTenantIdentifierValue
    //   getJdbcBatchSize / setJdbcBatchSize / getFetchBatchSize / setFetchBatchSize
    //   isSubselectFetchingEnabled / setSubselectFetchingEnabled
    //   lock(Object, LockMode, LockOption...) / lock(Object, LockOptions)
    // -------------------------------------------------------------------------
}
