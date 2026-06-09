# Hibernate Session Injection in Quarkus - Detailed Explanation

## Overview

This document explains how Hibernate Sessions are injected in Quarkus and what happens when you try to perform write operations outside of a transaction.

## How Session Injection Works

### 1. Build-Time Setup (Deployment Phase)

In `HibernateOrmCdiProcessor.java`, Quarkus creates a **synthetic CDI bean** for the Session:

```java
producer.produce(createSyntheticBean(puRef,
        Session.class, SESSION_EXPOSED_TYPES, false)
        .createWith(recorder.sessionSupplier(puRef.persistenceUnitName))
        .addInjectionPoint(ClassType.create(TransactionSessions.class))
        .done());
```

This creates an `@ApplicationScoped` bean that:
- Exposes both `EntityManager` and `Session` types
- Is qualified with `@Default` or `@PersistenceUnit("name")`
- Delegates creation to the runtime recorder

### 2. Runtime Creation

The `HibernateOrmRecorder` creates the actual injectable Session:

```java
public Function<SyntheticCreationalContext<Session>, Session> sessionSupplier(String persistenceUnitName) {
    return context -> {
        TransactionSessions transactionSessions = context.getInjectedReference(TransactionSessions.class);
        return new SessionLazyDelegator(() -> transactionSessions.getSession(persistenceUnitName));
    };
}
```

The injected Session is a **lazy delegating proxy** that wraps a `TransactionScopedSession`.

### 3. Transaction-Scoped Session

`TransactionScopedSession` is the key component. It's **not** a real session, but a **proxy** that implements `Session` and acquires the actual session on demand:

```java
SessionResult acquireSession() {
    if (isInTransaction()) {
        // Get or create session from transaction registry
        Session session = (Session) transactionSynchronizationRegistry.getResource(sessionKey);
        if (session != null) {
            return new SessionResult(session, false, true);
        }
        Session newSession = jtaSessionOpener.openSession();
        transactionSynchronizationRegistry.putResource(sessionKey, newSession);
        return new SessionResult(newSession, false, true);
    } else if (requestScopedSessionEnabled) {
        // Use request-scoped session
        RequestScopedSessionHolder holder = this.requestScopedSessions.get();
        return new SessionResult(holder.getOrCreateSession(unitName, sessionFactory), false, false);
    } else {
        throw new ContextNotActiveException(
            "Cannot use the EntityManager/Session because no transaction is active.");
    }
}
```

## What Happens When You Try to Write Outside a Transaction

### Scenario 1: Request-Scoped Disabled (Default)

**Configuration:** Default (no config needed)

**Code:**
```java
@Inject
Session session;

public void doWrite() {
    MyEntity entity = new MyEntity("test");
    session.persist(entity);  // FAILS HERE
}
```

**Result:**
```
jakarta.enterprise.context.ContextNotActiveException:
Cannot use the EntityManager/Session because no transaction is active.
Consider adding @Transactional to your method to automatically activate a transaction,
or set 'quarkus.hibernate-orm.request-scoped.enabled' to 'true' if you have valid
reasons not to use transactions.
```

**Why:** Without a transaction and without request-scoped sessions, there's no valid context to acquire a session.

---

### Scenario 2: Request-Scoped Enabled, persist() Only

**Configuration:** `quarkus.hibernate-orm.request-scoped.enabled=true`

**Code:**
```java
@Inject
Session session;

public void doWrite() {
    Arc.container().requestContext().activate();
    try {
        MyEntity entity = new MyEntity("test");
        session.persist(entity);  // SUCCEEDS (but doesn't persist to DB)
    } finally {
        Arc.container().requestContext().terminate();
    }
}
```

**Result:** ✅ **No exception** - BUT the entity is never saved to the database!

**Why:** With request-scoped sessions, `persist()` adds the entity to the in-memory session's persistence context. However, without a transaction, nothing is flushed to the database. When the request context terminates, the session is closed and the entity is lost.

---

### Scenario 3: Request-Scoped Enabled, persist() + flush()

**Configuration:** `quarkus.hibernate-orm.request-scoped.enabled=true`

**Code:**
```java
@Inject
Session session;

public void doWrite() {
    Arc.container().requestContext().activate();
    try {
        MyEntity entity = new MyEntity("test");
        session.persist(entity);  // Succeeds
        session.flush();          // FAILS HERE
    } finally {
        Arc.container().requestContext().terminate();
    }
}
```

**Result:**
```
jakarta.persistence.TransactionRequiredException:
Transaction is not active, consider adding @Transactional to your method
to automatically activate one.
```

**Why:** `flush()` tries to synchronize the persistence context with the database, which requires an active transaction. The check happens in `TransactionScopedSession.acquireSession()`:

```java
if (!emr.allowModification) {
    throw new TransactionRequiredException(TRANSACTION_IS_NOT_ACTIVE);
}
```

---

### Scenario 4: The Correct Way - Using @Transactional

**Code:**
```java
@Inject
Session session;

@Transactional
public void doWrite() {
    MyEntity entity = new MyEntity("test");
    session.persist(entity);
    session.flush();  // ✅ Works!
}
```

**Result:** ✅ **Success** - Entity is persisted to the database.

**Why:** 
1. `@Transactional` starts a JTA transaction
2. `acquireSession()` detects the active transaction
3. A session is created and associated with the transaction via `TransactionSynchronizationRegistry`
4. The session is automatically flushed and closed when the transaction completes
5. Changes are committed to the database

## Complete Test Example

Here's a complete test demonstrating all scenarios:

```java
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.inject.Inject;
import jakarta.persistence.TransactionRequiredException;
import jakarta.transaction.Transactional;

import org.hibernate.Session;
import org.junit.jupiter.api.Test;

import io.quarkus.arc.Arc;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class SessionWriteTest {

    @Inject
    Session session;

    @Test
    public void testPersistAndFlushWithoutTransaction_RequestScopeEnabled() {
        // Requires: quarkus.hibernate-orm.request-scoped.enabled=true
        Arc.container().requestContext().activate();
        try {
            assertThatThrownBy(() -> {
                MyEntity entity = new MyEntity("test");
                session.persist(entity);  // Succeeds
                session.flush();          // Fails with TransactionRequiredException
            })
            .isInstanceOf(TransactionRequiredException.class)
            .hasMessageContaining("Transaction is not active");
        } finally {
            Arc.container().requestContext().terminate();
        }
    }

    @Test
    @Transactional
    public void testPersistAndFlushWithTransaction() {
        // This works correctly
        MyEntity entity = new MyEntity("test");
        session.persist(entity);
        session.flush();
        // Entity is saved to database when transaction commits
    }
}
```

## Key Takeaways

1. **Injected Session is a Proxy**: The `@Inject Session` is actually a `TransactionScopedSession` proxy, not a real Hibernate session.

2. **Session Acquired Per-Operation**: Every method call on the injected session triggers `acquireSession()` to get the real session.

3. **Three Session Sources**:
   - **Transaction-scoped** (preferred): Session tied to JTA transaction
   - **Request-scoped** (fallback): Session tied to CDI request context
   - **None** (error): Throws exception if neither is active

4. **Write Operations Require Transactions**: Operations like `flush()`, `persist()` with modifications, `merge()`, etc., require an active transaction.

5. **Request-Scoped is Read-Only**: While `request-scoped.enabled=true` allows session access outside transactions, write operations still fail at flush time.

6. **Always Use @Transactional for Writes**: For any database modifications, use `@Transactional` to ensure proper transaction management.

## Architecture Benefits

This design provides:
- **Safety**: Prevents accidental data loss from unflushed changes
- **Clarity**: Clear error messages guide users to the correct solution
- **Flexibility**: Supports both transactional and read-only use cases
- **Thread-Safety**: Proper session isolation per transaction
- **Automatic Lifecycle**: Sessions are created and cleaned up automatically
