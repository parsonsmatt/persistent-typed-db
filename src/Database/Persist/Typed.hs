{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

{- | This module defines types and helpers for type-safe access to multiple
database schema.
-}
module Database.Persist.Typed (
    -- * Schema Definition
    mkSqlSettingsFor,
    SqlFor (..),
    BackendKey (..),

    -- * Specialized aliases
    SqlPersistTFor,
    ConnectionPoolFor,
    SqlPersistMFor,

    -- * Running specialized queries
    runSqlPoolFor,
    runSqlConnFor,

    -- * Specializing and generalizing
    generalizePool,
    specializePool,
    generalizeQuery,
    specializeQuery,
    generalizeSqlBackend,
    specializeSqlBackend,

    -- * Key functions
    toSqlKeyFor,
    fromSqlKeyFor,
) where

import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Logger (NoLoggingT)
import Control.Monad.Trans.Reader (ReaderT(..), withReaderT)
import Control.Monad.Trans.Resource (MonadUnliftIO, ResourceT)
import Data.Acquire (Acquire)
import qualified Data.Aeson as A
import Data.Coerce (Coercible, coerce)
import Data.Conduit (ConduitM)
import Data.Constraint (withDict, (:-))
import Data.Constraint.Unsafe (unsafeCoerceConstraint)
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Pool (Pool)
import Database.Persist.Sql hiding
       (deleteWhereCount, orderClause, updateWhereCount)
import Database.Persist.Sql.Types.Internal (IsPersistBackend(..))
import Database.Persist.TH (MkPersistSettings, mkPersistSettings)
import Language.Haskell.TH (Name, Type(..))
import Web.HttpApiData (FromHttpApiData, ToHttpApiData)
import Web.PathPieces (PathPiece)

#if MIN_VERSION_persistent(2,14,0)
import Database.Persist.Class.PersistEntity (SafeToInsert)
#else
import GHC.Exts (Constraint)
#endif

{- | A wrapper around 'SqlBackend' type. To specialize this to a specific
database, fill in the type parameter.

@since 0.0.1.0
-}
newtype SqlFor db = SqlFor {unSqlFor :: SqlBackend}

instance BackendCompatible SqlBackend (SqlFor db) where
    projectBackend = unSqlFor

{- | This type signature represents a database query for a specific database.
You will likely want to specialize this to your own application for
readability:

@
data MainDb

type MainQueryT = 'SqlPersistTFor' MainDb

getStuff :: 'MonadIO' m => StuffId -> MainQueryT m (Maybe Stuff)
@

@since 0.0.1.0
-}
type SqlPersistTFor db = ReaderT (SqlFor db)

{- | A 'Pool' of database connections that are specialized to a specific
database.

@since 0.0.1.0
-}
type ConnectionPoolFor db = Pool (SqlFor db)

--

{- | A specialization of 'SqlPersistM' that uses the underlying @db@ database
type.

@since 0.0.1.0
-}
type SqlPersistMFor db = ReaderT (SqlFor db) (NoLoggingT (ResourceT IO))

{- | Specialize a query to a specific database. You should define aliases for
this function for each database you use.

@
data MainDb

data AccountDb

mainQuery :: 'ReaderT' 'SqlBackend' m a -> 'ReaderT' ('SqlFor' MainDb) m a
mainQuery = 'specializeQuery'

accountQuery :: 'ReaderT' 'SqlBackend' m a -> 'ReaderT' ('SqlFor' AccountDb) m a
accountQuery = 'specializeQuery'
@

@since 0.0.1.0
-}
specializeQuery :: forall db m a. SqlPersistT m a -> SqlPersistTFor db m a
specializeQuery = withReaderT unSqlFor

{- | Generalizes a query from a specific database to one that is database
agnostic.

@since 0.0.1.0
-}
generalizeQuery :: forall db m a. SqlPersistTFor db m a -> SqlPersistT m a
generalizeQuery = withReaderT SqlFor

{- | Use the 'SqlFor' type for the database connection backend. Use this instead
of 'sqlSettings' and provide a quoted type name.

@
data MainDb

share [ mkPersist (mkSqlSettingsFor ''MainDb), mkMigrate "migrateAll" ] [persistLowerCase|

User
    name Text
    age  Int

    deriving Show Eq
|]
@

The entities generated will have the 'PersistEntityBackend' defined to be
@'SqlFor' MainDb@ instead of 'SqlBackend'. This is what provides the type
safety.

@since 0.0.1.0
-}
mkSqlSettingsFor :: Name -> MkPersistSettings
mkSqlSettingsFor n = mkPersistSettings (AppT (ConT ''SqlFor) (ConT n))

{- | Persistent's @toSqlKey@ and @fromSqlKey@ hardcode the 'SqlBackend', so we
have to reimplement them here.

@since 0.0.1.0
-}
toSqlKeyFor :: (ToBackendKey (SqlFor backend) record) => Int64 -> Key record
toSqlKeyFor = fromBackendKey . SqlForKey . SqlBackendKey

{- | Persistent's @toSqlKey@ and @fromSqlKey@ hardcode the 'SqlBackend', so we
have to reimplement them here.

@since 0.0.1.0
-}
fromSqlKeyFor :: ToBackendKey (SqlFor backend) record => Key record -> Int64
fromSqlKeyFor = unSqlBackendKey . unSqlForKey . toBackendKey

{- | Specialize a 'ConnectionPool' to a @'Pool' ('SqlFor' db)@. You should apply
this whenever you create or initialize the database connection pooling to
avoid potentially mixing the database pools up.

@since 0.0.1.0
-}
specializePool :: ConnectionPool -> ConnectionPoolFor db
specializePool = coerce

{- | Generalize a @'Pool' ('SqlFor' db)@ to an ordinary 'ConnectionPool'. This
renders the pool unusable for model-specific code that relies on the type
safety, but allows you to use it for general-purpose SQL queries.

@since 0.0.1.0
-}
generalizePool :: ConnectionPoolFor db -> ConnectionPool
generalizePool = coerce

{- | Specializes a 'SqlBackend' for a specific database.

@since 0.0.1.0
-}
specializeSqlBackend :: SqlBackend -> SqlFor db
specializeSqlBackend = SqlFor

{- | Generalizes a 'SqlFor' backend to be database agnostic.

@since 0.0.1.0
-}
generalizeSqlBackend :: SqlFor db -> SqlBackend
generalizeSqlBackend = unSqlFor

{- | Run a 'SqlPersistTFor' action on an appropriate database.

@since 0.0.1.0
-}
runSqlPoolFor ::
    MonadUnliftIO m =>
    SqlPersistTFor db m a ->
    ConnectionPoolFor db ->
    m a
runSqlPoolFor query conn =
    runSqlPool (generalizeQuery query) (generalizePool conn)

{- | Run a 'SqlPersistTFor' action on the appropriate database connection.

@since 0.0.1.0
-}
runSqlConnFor ::
    MonadUnliftIO m =>
    SqlPersistTFor db m a ->
    SqlFor db ->
    m a
runSqlConnFor query conn =
    runSqlConn (generalizeQuery query) (generalizeSqlBackend conn)

-- The following instances are almost entirely copy-pasted from the Persistent
-- library for SqlBackend.
instance HasPersistBackend (SqlFor backend) where
    type BaseBackend (SqlFor backend) = SqlFor backend
    persistBackend = id

instance IsPersistBackend (SqlFor backend) where
    mkPersistBackend = id

instance PersistCore (SqlFor backend) where
    newtype BackendKey (SqlFor backend) = SqlForKey {unSqlForKey :: BackendKey SqlBackend}
        deriving
            ( Show
            , Read
            , Eq
            , Ord
            , Num
            , Integral
            , PersistField
            , PersistFieldSql
            , PathPiece
            , ToHttpApiData
            , FromHttpApiData
            , Real
            , Enum
            , Bounded
            , A.ToJSON
            , A.FromJSON
            )

downcastPersistRecordBackend ::
  forall record backend.
  Coercible (SqlFor backend) SqlBackend =>
  PersistRecordBackend record (SqlFor backend) :- PersistRecordBackend record SqlBackend
downcastPersistRecordBackend = unsafeCoerceConstraint

specializeQueryWithInstances ::
    forall record backend m r.
    PersistRecordBackend record (SqlFor backend) =>
    (PersistRecordBackend record SqlBackend => SqlPersistT m r) ->
    SqlPersistTFor backend m r
specializeQueryWithInstances action = specializeQuery (withDict (downcastPersistRecordBackend @record @backend) action)

instance PersistStoreRead (SqlFor backend) where
    get :: forall record m. (MonadIO m, PersistRecordBackend record (SqlFor backend)) => Key record -> SqlPersistTFor backend m (Maybe record)
    get k = specializeQueryWithInstances @record $ get k
    getMany ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        [Key record] ->
        SqlPersistTFor backend m (Map (Key record) record)
    getMany ks = specializeQueryWithInstances @record $ getMany ks

instance PersistStoreWrite (SqlFor backend) where
    insert :: forall record m. (MonadIO m, PersistRecordBackend record (SqlFor backend), MySafeToInsert record) => record -> SqlPersistTFor backend m (Key record)
    insert v = specializeQueryWithInstances @record $ insert v

    insert_ ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend), MySafeToInsert record) =>
        record ->
        SqlPersistTFor backend m ()
    insert_ v = specializeQueryWithInstances @record $ insert_ v

    insertMany :: forall record m. (MonadIO m, PersistRecordBackend record (SqlFor backend), MySafeToInsert record) => [record] -> SqlPersistTFor backend m [Key record]
    insertMany vs = specializeQueryWithInstances @record $ insertMany vs

    insertMany_ :: forall record m. (MonadIO m, PersistRecordBackend record (SqlFor backend), MySafeToInsert record) => [record] -> SqlPersistTFor backend m ()
    insertMany_ vs = specializeQueryWithInstances @record $ insertMany_ vs

    insertEntityMany :: forall record m. (MonadIO m, PersistRecordBackend record (SqlFor backend)) => [Entity record] -> SqlPersistTFor backend m ()
    insertEntityMany es = specializeQueryWithInstances @record $ insertEntityMany es

    insertKey ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        Key record ->
        record ->
        SqlPersistTFor backend m ()
    insertKey k v = specializeQueryWithInstances @record $ insertKey k v

    repsert ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        Key record ->
        record ->
        SqlPersistTFor backend m ()
    repsert k v = specializeQueryWithInstances @record $ repsert k v

    repsertMany ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        [(Key record, record)] ->
        SqlPersistTFor backend m ()
    repsertMany kvs = specializeQueryWithInstances @record $ repsertMany kvs

    replace ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        Key record ->
        record ->
        SqlPersistTFor backend m ()
    replace k v = specializeQueryWithInstances @record $ replace k v

    delete ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        Key record ->
        SqlPersistTFor backend m ()
    delete k = specializeQueryWithInstances @record $ delete k

    update :: forall record m. (MonadIO m, PersistRecordBackend record (SqlFor backend)) => Key record -> [Update record] -> SqlPersistTFor backend m ()
    update k us = specializeQueryWithInstances @record $ update k us

    updateGet ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        Key record ->
        [Update record] ->
        SqlPersistTFor backend m record
    updateGet k us = specializeQueryWithInstances @record $ updateGet k us

-- orphaned instance for convenience of modularity
instance PersistQueryRead (SqlFor backend) where
    selectSourceRes ::
        forall record m1 m2.
        (PersistRecordBackend record (SqlFor backend), MonadIO m1, MonadIO m2) =>
        [Filter record] ->
        [SelectOpt record] ->
        SqlPersistTFor backend m1 (Acquire (ConduitM () (Entity record) m2 ()))
    selectSourceRes fs ss = specializeQueryWithInstances @record $ selectSourceRes fs ss

    selectFirst ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        [Filter record] ->
        [SelectOpt record] ->
        SqlPersistTFor backend m (Maybe (Entity record))
    selectFirst fs ss = specializeQueryWithInstances @record $ selectFirst fs ss

    selectKeysRes ::
        forall record m1 m2.
        (MonadIO m1, MonadIO m2, PersistRecordBackend record (SqlFor backend)) =>
        [Filter record] ->
        [SelectOpt record] ->
        SqlPersistTFor backend m1 (Acquire (ConduitM () (Key record) m2 ()))
    selectKeysRes fs ss = specializeQueryWithInstances @record $ selectKeysRes fs ss

    count ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        [Filter record] ->
        SqlPersistTFor backend m Int
    count fs = specializeQueryWithInstances @record $ count fs

    exists ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        [Filter record] ->
        SqlPersistTFor backend m Bool
    exists fs = specializeQueryWithInstances @record $ exists fs

instance PersistUniqueWrite (SqlFor backend) where
    deleteBy ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        Unique record ->
        SqlPersistTFor backend m ()
    deleteBy k = specializeQueryWithInstances @record $ deleteBy k
    insertUnique ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend), SafeToInsert record) =>
        record ->
        SqlPersistTFor backend m (Maybe (Key record))
    insertUnique v = specializeQueryWithInstances @record $ insertUnique v
    insertUnique_ ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend), SafeToInsert record) =>
        record ->
        SqlPersistTFor backend m (Maybe ())
    insertUnique_ v = specializeQueryWithInstances @record $ insertUnique_ v
    upsert ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend), OnlyOneUniqueKey record, MySafeToInsert record) =>
        record ->
        [Update record] ->
        SqlPersistTFor backend m (Entity record)
    upsert v us = specializeQueryWithInstances @record $ upsert v us
    upsertBy ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend), MySafeToInsert record) =>
        Unique record ->
        record ->
        [Update record] ->
        SqlPersistTFor backend m (Entity record)
    upsertBy k v us = specializeQueryWithInstances @record $ upsertBy k v us

    putMany ::
        forall record m.
        ( MonadIO m
        , PersistRecordBackend record (SqlFor backend)
        , MySafeToInsert record
        ) =>
        [record] ->
        SqlPersistTFor backend m ()
    putMany vs = specializeQueryWithInstances @record $ putMany vs

instance PersistUniqueRead (SqlFor backend) where
    getBy ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        Unique record ->
        SqlPersistTFor backend m (Maybe (Entity record))
    getBy k = specializeQueryWithInstances @record $ getBy k
    existsBy ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        Unique record ->
        SqlPersistTFor backend m Bool
    existsBy k = specializeQueryWithInstances @record $ existsBy k

instance PersistQueryWrite (SqlFor backend) where
    updateWhere ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        [Filter record] ->
        [Update record] ->
        SqlPersistTFor backend m ()
    updateWhere fs us = specializeQueryWithInstances @record $ updateWhere fs us

    deleteWhere ::
        forall record m.
        (MonadIO m, PersistRecordBackend record (SqlFor backend)) =>
        [Filter record] ->
        SqlPersistTFor backend m ()
    deleteWhere fs = specializeQueryWithInstances @record $ deleteWhere fs

type MySafeToInsert a =
#if MIN_VERSION_persistent(2,14,0)
    SafeToInsert a
#else
    () :: Constraint
#endif
