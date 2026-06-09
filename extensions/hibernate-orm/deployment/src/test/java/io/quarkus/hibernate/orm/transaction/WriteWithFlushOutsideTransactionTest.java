package io.quarkus.hibernate.orm.transaction;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.inject.Inject;
import jakarta.persistence.TransactionRequiredException;

import org.hibernate.Session;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.arc.Arc;
import io.quarkus.hibernate.orm.MyEntity;
import io.quarkus.hibernate.orm.naming.PrefixPhysicalNamingStrategy;
import io.quarkus.test.QuarkusUnitTest;

/**
 * Tests what happens when you try to persist an entity and call flush()
 * outside of a transaction using an injected Hibernate Session.
 */
public class WriteWithFlushOutsideTransactionTest {

    /**
     * Test with request-scoped sessions DISABLED (default).
     * Should fail immediately when trying to persist, even before flush.
     */
    public static class RequestScopeDisabled {

        @RegisterExtension
        static QuarkusUnitTest runner = new QuarkusUnitTest()
                .withApplicationRoot((jar) -> jar
                        .addClasses(MyEntity.class, PrefixPhysicalNamingStrategy.class)
                        .addAsResource(EmptyAsset.INSTANCE, "import.sql"))
                .overrideConfigKey("quarkus.hibernate-orm.request-scoped.enabled", "false");

        @Inject
        Session session;

        @Test
        public void testPersistWithoutTransaction() {
            // Without request scope, even activating request context won't help
            Arc.container().requestContext().activate();
            try {
                assertThatThrownBy(() -> {
                    MyEntity entity = new MyEntity("test-entity");
                    session.persist(entity);
                })
                        .isInstanceOf(ContextNotActiveException.class)
                        .hasMessageContaining("Cannot use the EntityManager/Session because no transaction is active")
                        .hasMessageContaining("Consider adding @Transactional to your method");
            } finally {
                Arc.container().requestContext().terminate();
            }
        }

        @Test
        public void testFlushWithoutTransaction() {
            Arc.container().requestContext().activate();
            try {
                // Even just calling flush without persist should fail
                assertThatThrownBy(() -> session.flush())
                        .isInstanceOf(ContextNotActiveException.class)
                        .hasMessageContaining("Cannot use the EntityManager/Session because no transaction is active");
            } finally {
                Arc.container().requestContext().terminate();
            }
        }
    }

    /**
     * Test with request-scoped sessions ENABLED.
     * Both persist() and flush() require an active transaction and will throw TransactionRequiredException.
     */
    public static class RequestScopeEnabled {

        @RegisterExtension
        static QuarkusUnitTest runner = new QuarkusUnitTest()
                .withApplicationRoot((jar) -> jar
                        .addClasses(MyEntity.class, PrefixPhysicalNamingStrategy.class)
                        .addAsResource(EmptyAsset.INSTANCE, "import.sql"))
                .overrideConfigKey("quarkus.hibernate-orm.request-scoped.enabled", "true");

        @Inject
        Session session;

        @Test
        public void testPersistAndFlushWithoutTransaction() {
            Arc.container().requestContext().activate();
            try {
                // persist() itself rejects writes outside a transaction — the proxy enforces this
                assertThatThrownBy(() -> {
                    MyEntity entity = new MyEntity("test-entity");
                    session.persist(entity);
                })
                        .isInstanceOf(TransactionRequiredException.class)
                        .hasMessageContaining(
                                "Transaction is not active, consider adding @Transactional to your method to automatically activate one");
            } finally {
                Arc.container().requestContext().terminate();
            }
        }

        @Test
        public void testFlushAloneWithoutTransaction() {
            Arc.container().requestContext().activate();
            try {
                // flush() also rejects without a transaction
                assertThatThrownBy(() -> session.flush())
                        .isInstanceOf(TransactionRequiredException.class)
                        .hasMessageContaining("Transaction is not active");
            } finally {
                Arc.container().requestContext().terminate();
            }
        }
    }

    /**
     * Test without activating request context at all.
     * Should fail immediately.
     */
    public static class NoRequestContext {

        @RegisterExtension
        static QuarkusUnitTest runner = new QuarkusUnitTest()
                .withApplicationRoot((jar) -> jar
                        .addClasses(MyEntity.class, PrefixPhysicalNamingStrategy.class)
                        .addAsResource(EmptyAsset.INSTANCE, "import.sql"))
                .overrideConfigKey("quarkus.hibernate-orm.request-scoped.enabled", "true");

        @Inject
        Session session;

        @Test
        public void testPersistWithoutRequestContextOrTransaction() {
            // Don't activate request context
            assertThatThrownBy(() -> {
                MyEntity entity = new MyEntity("test-entity");
                session.persist(entity);
            })
                    .isInstanceOf(ContextNotActiveException.class)
                    .hasMessageContaining("Cannot use the EntityManager/Session")
                    .hasMessageContaining("neither a transaction nor a CDI request context is active");
        }
    }
}
