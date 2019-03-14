# `persistent-typed-db`

[![Build Status](https://travis-ci.org/parsonsmatt/persistent-typed-db.svg?branch=master)](https://travis-ci.org/parsonsmatt/persistent-typed-db)

This library defines an alternate `SqlBackend` type for the Haskell [persistent][] database library.
The type has a phantom type parameter which allows you to write queries against multiple databases safely.

# The Problem

The `persistent` library uses a "handle pattern" with the `SqlBackend` type.
The `SqlBackend` (or `Pool SqlBackend`) is used to provide access to "The Database."
To access the database, you generally use a function like:

```haskell
runDB :: SqlPersistT m a -> App a
```

However, you may have two (or more!) databases.
You might have multiple database "runner" functions, like this:

```haskell
runMainDB
    :: SqlPersistT m a -> App a
runAuxDB
    :: SqlPersistT m a -> App a
```

Unfortunately, this isn't safe.
The schemas differ, and there's nothing preventing you from using the wrong runner for the query at hand.

This library allows you to differentiate between database schemata.
To demonstrate usage, we'll start with a pair of ordinary `persistent` quasiquoter blocks:

```haskell
share [ mkPersist sqlSettings, mkMigrate "migrateAll" ] [persistLowerCase|

User
    name Text
    age  Int

    deriving Show Eq
|]

share [ mkPersist sqlSettings, mkMigrate "migrateAll" ] [persistLowerCase|

AuxRecord
    createdAt UTCTime
    reason    Text

    deriving Show Eq
|]
```

These two definitions correspond to different databases.
The `persistent` library uses the `SqlPersistT m` monad transformer for queries.
This type is a synonym for `ReaderT SqlBackend m`.
Many of the functions defined in `persistent` have a signature like this:

```haskell
get :: (MonadIO m, PersistEntityBackend record ~ backend)
    => Key record
    -> ReaderT backend m (Maybe record)
```

It requires that the entity is compatible with the query monad.
We're going to substitute `User` for the type variable `record`.
In the initial schema definition, the `PersistEntityBackend User` is defined as `SqlBackend`.
So the type of `get`, in the original definition, is this:

```haskell
get :: (MonadIO m, PersistEntityBackend User ~ SqlBackend)
    => Key record
    -> ReaderT SqlBackend m (Maybe User)
```

If we look at the type of `get` specialized to `AuxRecord`, we see this:

```haskell
get :: (MonadIO m, PersistEntityBackend AuxRecord ~ SqlBackend)
    => Key record
    -> ReaderT SqlBackend m (Maybe AuxRecord)
```

This means that we might be able to write a query like this:

```haskell
impossibleQuery
    :: MonadIO m
    => SqlPersistT m (Maybe User, Maybe AuxRecord)
impossibleQuery = do
  muser <- get (UserKey 1)
  maux <- get (AuxRecordKey 1)
  pure (muser, maux)
```

This query will fail at runtime, since the entities exist on different schemata.
Likewise, there's nothing in the types to stop you from running a query against
the wrong backend:

```haskell
app = do
    runMainDB $ get (AuxRecordKey 3)
    runAuxDb $ get (UserKey 3)
```

# The Solution

Let's solve this problem.

## Declaring the Schema

We are going to create an empty datatype tag for each schema, and then we're going to use `mkSqlSettingsFor` instead of `sqlSettings`.

```haskell
data MainDb
data AuxDb

share [ mkPersist (mkSqlSettingsFor ''MainDb), mkMigrate "migrateAll" ] [persistLowerCase|

User
    name Text
    age  Int

    deriving Show Eq
|]

share [ mkPersist (mkSqlSettingsFor ''AuxDb), mkMigrate "migrateAll" ] [persistLowerCase|

AuxRecord
    createdAt UTCTime
    reason    Text

    deriving Show Eq
|]
```

This changes the type of the `PersistEntityBackend record` for each entity defined in the QuasiQuoter.
The previous type of `PersistEntityBackend User` was `SqlBackend`, but with this change, it is now `SqlBackendFor MainDb`.
Likewise, the type of `PersistEntityBackend AuxRecord` has become `SqlBackendFor AuxDb`.

## Using the Schema

Let's look at the new type of `get` for these two records:

```haskell
get :: (MonadIO m, PersistEntityBackend User ~ SqlBackendFor MainDb)
    => Key record
    -> ReaderT (SqlBackendFor MainDb) m (Maybe User)

get :: (MonadIO m, PersistEntityBackend AuxRecord ~ SqlBackendFor AuxDb)
    => Key record
    -> ReaderT (SqlBackendFor AuxDb) m (Maybe AuxRecord)
```

Now that the monad type is different, we can't use them in the same query.
Our previous `impossibleQuery` now fails with a type error.

The `persistent-typed-db` library defines a type synonym for `ReaderT`.
It is similar to the `SqlPersistT` synonym:

```haskell
type SqlPersistT       = ReaderT SqlBackend
type SqlPersistTFor db = ReaderT (SqlBackendFor db)
```

When using this library, it is a good idea to define a type snynonym for your databases as well.
So we might also write:

```haskell
type MainDbT = SqlPersistTFor MainDb
type AuxDbT  = SqlPersistTFor AuxDb
```

The type of our runner functions has changed, as well.
Before, we accepted a `SqlPersistT`, but now, we'll accept the right query type for the database:

```haskell
runMainDb :: MainDbT m a -> App a

runAuxDb  :: AuxDbT m a -> App a
```

We'll cover how to define these runner functions soon.

## Defining the Runner Function

`persistent` defines a function `runSqlPool` that is useful for running a SQL action.
The type is essentially this:

```haskell
runSqlPool
    :: (MonadUnliftIO m, IsSqlBackend backend)
    => ReaderT backend m a
    -> Pool backend
    -> m a
```

`persistent-typed-db` defines a function that is a drop in replacement for this, called `runSqlPoolFor`.

```haskell
runSqlPoolFor
    :: (MonadUnliftIO m)
    => SqlPersistTFor db m a
    -> ConnectionPoolFor db
    -> m a
```

It is defined by generalizing the input query and pool, and delegating to `runSqlPool`.

```haskell
runSqlPoolFor query conn =
    runSqlPool (generalizeQuery query) (generalizePool conn)
```

Sometimes, you'll have some function that is in `SqlPersistT` that you want to use on a specialized database.
This can occur with raw queries, like `rawSql` and friends, or other queries/actions that are not tied to a `PersistEntityBackend` type.
In this case, you'll want to use `specializeQuery`.
You will likely want to define type-specified helpers that are aliases for `specializeQuery`:

```haskell
toMainQuery :: SqlPersistT m a -> MainDbT m a
toMainQuery = specializeQuery

toAuxQuery :: SqlPersistT m a -> AuxDbT m a
toAuxQuery = specializeQuery
```

## Constructing the Pools

`persistent` (and the relevant database-specific libraries) define many functions for creating connections.
We'll use [`createPostgresqlPool`](https://hackage.haskell.org/package/persistent-postgresql-2.9.0/docs/Database-Persist-Postgresql.html#v:createPostgresqlPool) as an example.
This is one place where you do need to be careful, as you are tagging the database pool with the database type.

To create a specific database pool, we'll map `specializePool` over the result
of `createPostgresqlPool`:

```haskell
createPostgresqlPoolFor
    :: ConnectionString
    -> Int
    -> IO (ConnectionPoolFor db)
createPostgresqlPoolFor connStr i =
    specializePool <$> createPostgresqlPool connStr i
```

It is a good idea to make specialized variants of this function to improve type
inference and errors:

```haskell
createMainPool :: ConnectionString -> Int -> IO (ConnectionPoolFor MainDb)
createMainPool = createPostgresqlPoolFor

createAuxPool  :: ConnectionString -> Int -> IO (ConnectionPoolFor AuxDb)
createAuxPool  = createPostgresqlPoolFor
```

It is common to use `with`-style functions with these, as well.
These functions automate closure of the database resources.
We can specialize these functions similarly:

```haskell
withPoolFor
    :: forall db m a
     .  (MonadLogger m, MonadUnliftIO m)
    => ConnectionString
    -> Int
    -> (ConnectionPoolFor db -> m a)
    -> m a
withPoolFor connStr conns action =
    withPostgresqlPool connStr conns $ \genericPool ->
        action (specializePool genericPool)
```
