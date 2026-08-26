package io.quarkus.hibernate.orm.runtime.session;

import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.persistence.EntityManagerFactory;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.engine.spi.SessionLazyDelegator;

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
        checkBlocking();
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
}
