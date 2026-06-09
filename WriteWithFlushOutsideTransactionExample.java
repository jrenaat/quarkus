package example;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.TransactionRequiredException;

import org.hibernate.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.arc.Arc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Example test demonstrating what happens when you try to write (persist + flush)
 * an entity outside of a transaction using an injected Hibernate Session.
 *
 * This test assumes:
 * - quarkus.hibernate-orm.request-scoped.enabled=true is configured
 * - A simple @Entity class exists
 */
@ApplicationScoped
public class WriteWithFlushOutsideTransactionExample {

    @Inject
    Session session;  // Injected Hibernate Session (actually a TransactionScopedSession proxy)

    @BeforeEach
    public void activateRequestContext() {
        // Required when request-scoped sessions are enabled
        Arc.container().requestContext().activate();
    }

    @Test
    public void testPersistAndFlushWithoutTransaction() {
        // SCENARIO 1: Try to persist and flush without @Transactional

        assertThatThrownBy(() -> {
            MyEntity entity = new MyEntity("test-entity");

            // With request-scoped enabled, persist() will succeed
            // The entity is added to the session's persistence context
            session.persist(entity);

            // But flush() will fail because we're not in a transaction
            // flush() tries to synchronize the persistence context with the database
            // which requires a transaction
            session.flush();
        })
        .isInstanceOf(TransactionRequiredException.class)
        .hasMessageContaining("Transaction is not active")
        .hasMessageContaining("Consider adding @Transactional to your method");
    }

    @Test
    public void testFlushAloneWithoutTransaction() {
        // SCENARIO 2: Even calling flush() alone without persist fails

        assertThatThrownBy(() -> {
            session.flush();
        })
        .isInstanceOf(TransactionRequiredException.class)
        .hasMessageContaining("Transaction is not active");
    }

    @Test
    public void testPersistWithoutFlushSucceeds() {
        // SCENARIO 3: persist() without flush() will succeed
        // BUT the entity is never actually saved to the database

        MyEntity entity = new MyEntity("test-entity");

        // This succeeds - no exception thrown
        session.persist(entity);

        // However, the entity only exists in the in-memory session
        // It will be discarded when the request context is terminated
        // Nothing is written to the database
    }

    @Test
    public void testWithoutRequestScopedEnabled() {
        // SCENARIO 4: If quarkus.hibernate-orm.request-scoped.enabled=false
        // (which is the default), even persist() will fail immediately

        // This would throw:
        // jakarta.enterprise.context.ContextNotActiveException:
        //   Cannot use the EntityManager/Session because no transaction is active.
        //   Consider adding @Transactional to your method...
    }

    @AfterEach
    public void terminateRequestContext() {
        Arc.container().requestContext().terminate();
    }
}

// Example entity class
class MyEntity {
    private Long id;
    private String name;

    public MyEntity(String name) {
        this.name = name;
    }

    // getters/setters...
}

/**
 * SUMMARY OF BEHAVIOR:
 *
 * 1. With request-scoped DISABLED (default):
 *    - Any session operation fails with ContextNotActiveException
 *    - Message: "Cannot use the EntityManager/Session because no transaction is active"
 *
 * 2. With request-scoped ENABLED:
 *    - Read operations work (find, query, etc.)
 *    - persist() works BUT entity stays only in memory
 *    - flush() fails with TransactionRequiredException
 *    - Message: "Transaction is not active, consider adding @Transactional to your method"
 *
 * 3. The CORRECT way:
 *    @Transactional
 *    public void saveEntity(MyEntity entity) {
 *        session.persist(entity);
 *        session.flush();  // Now this works!
 *    }
 *
 * WHY THIS DESIGN?
 * - Quarkus uses a transaction-scoped Session proxy (TransactionScopedSession)
 * - Each method call on the injected Session triggers acquireSession()
 * - acquireSession() retrieves the real session from:
 *   a) TransactionSynchronizationRegistry (if in transaction)
 *   b) RequestScopedSessionHolder (if request-scoped enabled)
 *   c) Throws exception (if neither)
 * - Write operations (flush, persist with modifications) require a transaction
 * - This ensures proper transaction management and prevents accidental data loss
 */
