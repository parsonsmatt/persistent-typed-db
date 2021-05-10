{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module defines types and helpers for type-safe access to multiple
-- database schema.
module Database.Persist.Typed
    ( -- * Schema Definition
      mkSqlSettingsFor
    , SqlFor(..)
      -- * Specialized aliases
    , SqlPersistTFor
    , ConnectionPoolFor
    , SqlPersistMFor
      -- * Running specialized queries
    , runSqlPoolFor
    , runSqlConnFor
      -- * Specializing and generalizing
    , generalizePool
    , specializePool
    , generalizeQuery
    , specializeQuery
    , generalizeSqlBackend
    , specializeSqlBackend
      -- * Key functions
    , toSqlKeyFor
    , fromSqlKeyFor
    ) where

import Control.Exception hiding (throw)
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Logger (NoLoggingT)
import Control.Monad.Trans.Reader (ReaderT(..), ask, asks, withReaderT)
import Control.Monad.Trans.Resource (MonadUnliftIO, ResourceT)
import Data.Aeson as A
import Data.ByteString.Char8 (readInteger)
import Data.Coerce (coerce)
import Data.Conduit ((.|))
import qualified Data.Conduit.List as CL
import qualified Data.Foldable as Foldable
import Data.Int (Int64)
import Data.List (find, inits, transpose)
import qualified Data.List.NonEmpty as NEL
import Data.Maybe (isJust)
import Data.Monoid (mappend, (<>))
import Data.Pool (Pool)
import Data.Text (Text)
import qualified Data.Text as Text
import Database.Persist.Sql hiding
       (deleteWhereCount, filterClause, updateWhereCount)
import Database.Persist.Sql.Types.Internal (IsPersistBackend(..))
import Database.Persist.Sql.Util
import Database.Persist.SqlBackend.Internal
import Database.Persist.TH (MkPersistSettings, mkPersistSettings)
import Language.Haskell.TH (Name, Type(..))
import Web.HttpApiData (FromHttpApiData, ToHttpApiData)
import Web.PathPieces (PathPiece)

-- | A wrapper around 'SqlBackend' type. To specialize this to a specific
-- database, fill in the type parameter.
--
-- @since 0.0.1.0
newtype SqlFor db = SqlFor { unSqlFor :: SqlBackend }

instance BackendCompatible SqlBackend (SqlFor db) where
    projectBackend = unSqlFor

-- | This type signature represents a database query for a specific database.
-- You will likely want to specialize this to your own application for
-- readability:
--
-- @
-- data MainDb
--
-- type MainQueryT = 'SqlPersistTFor' MainDb
--
-- getStuff :: 'MonadIO' m => StuffId -> MainQueryT m (Maybe Stuff)
-- @
--
-- @since 0.0.1.0
type SqlPersistTFor db = ReaderT (SqlFor db)

-- | A 'Pool' of database connections that are specialized to a specific
-- database.
--
-- @since 0.0.1.0
type ConnectionPoolFor db = Pool (SqlFor db)
--
-- | A specialization of 'SqlPersistM' that uses the underlying @db@ database
-- type.
--
-- @since 0.0.1.0
type SqlPersistMFor db = ReaderT (SqlFor db) (NoLoggingT (ResourceT IO))

-- | Specialize a query to a specific database. You should define aliases for
-- this function for each database you use.
--
-- @
-- data MainDb
--
-- data AccountDb
--
-- mainQuery :: 'ReaderT' 'SqlBackend' m a -> 'ReaderT' ('SqlFor' MainDb) m a
-- mainQuery = 'specializeQuery'
--
-- accountQuery :: 'ReaderT' 'SqlBackend' m a -> 'ReaderT' ('SqlFor' AccountDb) m a
-- accountQuery = 'specializeQuery'
-- @
--
-- @since 0.0.1.0
specializeQuery :: forall db m a. SqlPersistT m a -> SqlPersistTFor db m a
specializeQuery = withReaderT unSqlFor

-- | Generalizes a query from a specific database to one that is database
-- agnostic.
--
-- @since 0.0.1.0
generalizeQuery :: forall db m a. SqlPersistTFor db m a -> SqlPersistT m a
generalizeQuery = withReaderT SqlFor

-- | Use the 'SqlFor' type for the database connection backend. Use this instead
-- of 'sqlSettings' and provide a quoted type name.
--
-- @
-- data MainDb
--
-- share [ mkPersist (mkSqlSettingsFor ''MainDb), mkMigrate "migrateAll" ] [persistLowerCase|
--
-- User
--     name Text
--     age  Int
--
--     deriving Show Eq
-- |]
-- @
--
-- The entities generated will have the 'PersistEntityBackend' defined to be
-- @'SqlFor' MainDb@ instead of 'SqlBackend'. This is what provides the type
-- safety.
--
-- @since 0.0.1.0
mkSqlSettingsFor :: Name -> MkPersistSettings
mkSqlSettingsFor n = mkPersistSettings (AppT (ConT ''SqlFor) (ConT n))

-- | Persistent's @toSqlKey@ and @fromSqlKey@ hardcode the 'SqlBackend', so we
-- have to reimplement them here.
--
-- @since 0.0.1.0
toSqlKeyFor :: (ToBackendKey (SqlFor a) record) => Int64 -> Key record
toSqlKeyFor = fromBackendKey . SqlForKey . SqlBackendKey

-- | Persistent's @toSqlKey@ and @fromSqlKey@ hardcode the 'SqlBackend', so we
-- have to reimplement them here.
--
-- @since 0.0.1.0
fromSqlKeyFor :: ToBackendKey (SqlFor a) record => Key record -> Int64
fromSqlKeyFor = unSqlBackendKey . unSqlForKey . toBackendKey

-- | Specialize a 'ConnectionPool' to a @'Pool' ('SqlFor' db)@. You should apply
-- this whenever you create or initialize the database connection pooling to
-- avoid potentially mixing the database pools up.
--
-- @since 0.0.1.0
specializePool :: ConnectionPool -> ConnectionPoolFor db
specializePool = coerce

-- | Generalize a @'Pool' ('SqlFor' db)@ to an ordinary 'ConnectionPool'. This
-- renders the pool unusable for model-specific code that relies on the type
-- safety, but allows you to use it for general-purpose SQL queries.
--
-- @since 0.0.1.0
generalizePool :: ConnectionPoolFor db -> ConnectionPool
generalizePool = coerce

-- | Specializes a 'SqlBackend' for a specific database.
--
-- @since 0.0.1.0
specializeSqlBackend :: SqlBackend -> SqlFor db
specializeSqlBackend = SqlFor

-- | Generalizes a 'SqlFor' backend to be database agnostic.
--
-- @since 0.0.1.0
generalizeSqlBackend :: SqlFor db -> SqlBackend
generalizeSqlBackend = unSqlFor

-- | Run a 'SqlPersistTFor' action on an appropriate database.
--
-- @since 0.0.1.0
runSqlPoolFor
    :: MonadUnliftIO m
    => SqlPersistTFor db m a
    -> ConnectionPoolFor db
    -> m a
runSqlPoolFor query conn =
    runSqlPool (generalizeQuery query) (generalizePool conn)

-- | Run a 'SqlPersistTFor' action on the appropriate database connection.
--
-- @since 0.0.1.0
runSqlConnFor
    :: MonadUnliftIO m
    => SqlPersistTFor db m a
    -> SqlFor db
    -> m a
runSqlConnFor query conn =
    runSqlConn (generalizeQuery query) (generalizeSqlBackend conn)

-- The following instances are almost entirely copy-pasted from the Persistent
-- library for SqlBackend.
instance HasPersistBackend (SqlFor a) where
    type BaseBackend (SqlFor a) = SqlFor a
    persistBackend = id

instance IsPersistBackend (SqlFor a) where
    mkPersistBackend = id

instance PersistCore (SqlFor a) where
    newtype BackendKey (SqlFor a) =
        SqlForKey { unSqlForKey :: BackendKey SqlBackend }
        deriving ( Show, Read, Eq, Ord, Num, Integral, PersistField
                 , PersistFieldSql, PathPiece, ToHttpApiData, FromHttpApiData
                 , Real, Enum, Bounded, A.ToJSON, A.FromJSON
                 )

instance PersistStoreRead (SqlFor a) where
    get k = do
        conn <- asks unSqlFor
        let t = entityDef $ dummyFromKey k
        let cols = Text.intercalate ","
                 $ map (connEscapeRawName conn . unFieldNameDB . fieldDB) $ entityFields t
            noColumns :: Bool
            noColumns = null $ entityFields t
        let wher = whereStmtForKey conn k
        let sql = Text.concat
                [ "SELECT "
                , if noColumns then "*" else cols
                , " FROM "
                , connEscapeRawName conn $ unEntityNameDB $ getEntityDBName t
                , " WHERE "
                , wher
                ]
        flip runReaderT conn $ withRawQuery sql (keyToValues k) $ do
            res <- CL.head
            case res of
                Nothing -> return Nothing
                Just vals ->
                    case fromPersistValues $ if noColumns then [] else vals of
                        Left e -> error $ "get " ++ show k ++ ": " ++ Text.unpack e
                        Right v -> return $ Just v

instance PersistStoreWrite (SqlFor a) where
    update _ [] = return ()
    update k upds = specializeQuery $ do
        conn <- ask
        let go'' n Assign = n <> "=?"
            go'' n Add = Text.concat [n, "=", n, "+?"]
            go'' n Subtract = Text.concat [n, "=", n, "-?"]
            go'' n Multiply = Text.concat [n, "=", n, "*?"]
            go'' n Divide = Text.concat [n, "=", n, "/?"]
            go'' _ (BackendSpecificUpdate up) = error $ Text.unpack $ "BackendSpecificUpdate" `Data.Monoid.mappend` up `mappend` "not supported"
        let go' (x, pu) = go'' (connEscapeRawName conn x) pu
        let wher = whereStmtForKey conn k
        let sql = Text.concat
                [ "UPDATE "
                , connEscapeRawName conn $ unEntityNameDB $ tableDBName $ recordTypeFromKey k
                , " SET "
                , Text.intercalate "," $ map (go' . go) upds
                , " WHERE "
                , wher
                ]
        rawExecute sql $
            map updatePersistValue upds `mappend` keyToValues k
      where
        go x = (unFieldNameDB $ fieldDB $ updateFieldDef x, updateUpdate x)

    insert val = specializeQuery $ do
        conn <- ask
        case connInsertSql conn t vals of
            ISRSingle sql -> withRawQuery sql vals $ do
                x <- CL.head
                case x of
                    Just [PersistInt64 i] -> case keyFromValues [PersistInt64 i] of
                        Left err -> error $ "SQL insert: keyFromValues: PersistInt64 " `mappend` show i `mappend` " " `mappend` Text.unpack err
                        Right k -> return k
                    Nothing -> error "SQL insert did not return a result giving the generated ID"
                    Just vals' -> case keyFromValues vals' of
                        Left _ -> error $ "Invalid result from a SQL insert, got: " ++ show vals'
                        Right k -> return k

            ISRInsertGet sql1 sql2 -> do
                rawExecute sql1 vals
                withRawQuery sql2 [] $ do
                    mm <- CL.head
                    let m = maybe
                              (Left $ "No results from ISRInsertGet: " `mappend` tshow (sql1, sql2))
                              Right mm

                    -- TODO: figure out something better for MySQL
                    let convert x =
                            case x of
                                [PersistByteString i] -> case readInteger i of -- mssql
                                                        Just (ret,"") -> [PersistInt64 $ fromIntegral ret]
                                                        _ -> x
                                _ -> x
                        -- Yes, it's just <|>. Older bases don't have the
                        -- instance for Either.
                        onLeft Left{} x = x
                        onLeft x _      = x

                    case m >>= (\x -> keyFromValues x `onLeft` keyFromValues (convert x)) of
                        Right k -> return k
                        Left err -> throw $ "ISRInsertGet: keyFromValues failed: " `mappend` err
            ISRManyKeys sql fs -> do
                rawExecute sql vals
                case entityPrimary t of
                   Nothing -> error $ "ISRManyKeys is used when Primary is defined " ++ show sql
                   Just pdef ->
                        let pks = map fieldHaskell $ NEL.toList $ compositeFields pdef
                            keyvals = map snd $ filter (\(a, _) -> let ret=isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ entityFields t) fs
                        in  case keyFromValues keyvals of
                                Right k -> return k
                                Left e  -> error $ "ISRManyKeys: unexpected keyvals result: " `mappend` Text.unpack e
      where
        tshow :: Show a => a -> Text
        tshow = Text.pack . show
        throw = liftIO . throwIO . userError . Text.unpack
        t = entityDef $ Just val
        vals = map toPersistValue $ toPersistFields val

    insertMany [] = return []
    insertMany vals = specializeQuery $ do
        conn <- ask

        case connInsertManySql conn of
            Nothing -> withReaderT SqlFor $ mapM insert vals
            Just insertManyFn ->
                case insertManyFn ent valss of
                    ISRSingle sql -> rawSql sql (concat valss)
                    _ -> error "ISRSingle is expected from the connInsertManySql function"
                where
                    ent = entityDef vals
                    valss = map (map toPersistValue . toPersistFields) vals

    insertEntityMany es' = specializeQuery $ do
        conn <- ask
        let entDef = entityDef $ map entityVal es'
        let columnNames = NEL.toList $ keyAndEntityColumnNames entDef conn
        runChunked (length columnNames) go es'
      where
        go = insrepHelper "INSERT"


    insertMany_ [] = return ()
    insertMany_ vals0 = specializeQuery $ do
        conn <- ask
        case connMaxParams conn of
            Nothing -> insertMany_' vals0
            Just maxParams -> do
                let chunkSize = maxParams `div` length (entityFields t)
                mapM_ insertMany_' (chunksOf chunkSize vals0)
      where
        insertMany_' vals = do
          conn <- ask
          let valss = map (map toPersistValue . toPersistFields) vals
          let sql = Text.concat
                  [ "INSERT INTO "
                  , connEscapeRawName conn (unEntityNameDB $ getEntityDBName t)
                  , "("
                  , Text.intercalate "," $ map (connEscapeRawName conn . unFieldNameDB . fieldDB) $ entityFields t
                  , ") VALUES ("
                  , Text.intercalate "),(" $ replicate (length valss) $ Text.intercalate "," $ map (const "?") (entityFields t)
                  , ")"
                  ]
          rawExecute sql (concat valss)

        t = entityDef vals0

    replace k val = do
        conn <- asks unSqlFor
        let t = entityDef $ Just val
        let wher = whereStmtForKey conn k
        let sql = Text.concat
                [ "UPDATE "
                , connEscapeRawName conn (unEntityNameDB $ getEntityDBName t)
                , " SET "
                , Text.intercalate "," (map (go conn . unFieldNameDB . fieldDB) $ entityFields t)
                , " WHERE "
                , wher
                ]
            vals = map toPersistValue (toPersistFields val) `mappend` keyToValues k
        specializeQuery $ rawExecute sql vals
      where
        go conn x = connEscapeRawName conn x `Text.append` "=?"

    insertKey k v = specializeQuery $ insrepHelper "INSERT" [Entity k v]

    repsert key value = do
        mExisting <- get key
        case mExisting of
          Nothing -> insertKey key value
          Just _  -> replace key value

    delete k = do
        conn <- asks unSqlFor
        specializeQuery $ rawExecute (sql conn) (keyToValues k)
      where
        wher conn = whereStmtForKey conn k
        sql conn = Text.concat
            [ "DELETE FROM "
            , connEscapeRawName conn $ unEntityNameDB $ tableDBName $ recordTypeFromKey k
            , " WHERE "
            , wher conn
            ]

-- orphaned instance for convenience of modularity
instance PersistQueryRead (SqlFor a) where
    exists filts =
        (>0) <$> count filts
    count filts = specializeQuery $ do
        conn <- ask
        let wher = if null filts
                    then ""
                    else filterClause False (SqlFor conn) filts
        let sql = mconcat
                [ "SELECT COUNT(*) FROM "
                , connEscapeRawName conn $ unEntityNameDB $ getEntityDBName t
                , wher
                ]
        withRawQuery sql (getFiltsValues (SqlFor conn) filts) $ do
            mm <- CL.head
            case mm of
              Just [PersistInt64 i] -> return $ fromIntegral i
              Just [PersistDouble i] ->return $ fromIntegral (truncate i :: Int64) -- gb oracle
              Just [PersistByteString i] -> case readInteger i of -- gb mssql
                                              Just (ret,"") -> return $ fromIntegral ret
                                              xs -> error $ "invalid number i["++show i++"] xs[" ++ show xs ++ "]"
              Just xs -> error $ "count:invalid sql  return xs["++show xs++"] sql["++show sql++"]"
              Nothing -> error $ "count:invalid sql returned nothing sql["++show sql++"]"
      where
        t = entityDef $ dummyFromFilts filts

    selectSourceRes filts opts = specializeQuery $ do
        conn <- ask
        srcRes <- rawQueryRes (sql conn) (getFiltsValues (SqlFor conn) filts)
        return $ fmap (.| CL.mapM parse) srcRes
      where
        (limit, offset, orders) = limitOffsetOrder opts

        parse vals = case parseEntityValues t vals of
                       Left s    -> liftIO $ throwIO $ PersistMarshalError s
                       Right row -> return row
        t = entityDef $ dummyFromFilts filts
        wher conn = if null filts
                    then ""
                    else filterClause False (SqlFor conn) filts
        ord conn =
            case map (orderClause False conn) orders of
                []   -> ""
                ords -> " ORDER BY " <> Text.intercalate "," ords
        cols = Text.intercalate ", " . entityColumnNames t
        sql conn = connLimitOffset conn (limit,offset) $ mconcat
            [ "SELECT "
            , cols conn
            , " FROM "
            , connEscapeRawName conn $ unEntityNameDB $ getEntityDBName t
            , wher conn
            , ord (SqlFor conn)
            ]

    selectKeysRes filts opts = specializeQuery $ do
        conn <- ask
        srcRes <- rawQueryRes (sql conn) (getFiltsValues (SqlFor conn) filts)
        return $ fmap (.| CL.mapM parse) srcRes
      where
        t = entityDef $ dummyFromFilts filts
        cols conn = Text.intercalate "," $ dbIdColumns conn t


        wher conn = if null filts
                    then ""
                    else filterClause False (SqlFor conn) filts
        sql conn = connLimitOffset conn (limit,offset) $ mconcat
            [ "SELECT "
            , cols conn
            , " FROM "
            , connEscapeRawName conn $ unEntityNameDB $ getEntityDBName t
            , wher conn
            , ord conn
            ]

        (limit, offset, orders) = limitOffsetOrder opts

        ord conn =
            case map (orderClause False (SqlFor conn)) orders of
                []   -> ""
                ords -> " ORDER BY " <> Text.intercalate "," ords

        parse xs = do
            keyvals <- case entityPrimary t of
                      Nothing ->
                        case xs of
                           [PersistInt64 x] -> return [PersistInt64 x]
                           [PersistDouble x] -> return [PersistInt64 (truncate x)] -- oracle returns Double
                           _ -> return xs
                      Just pdef ->
                           let pks = map fieldHaskell $ NEL.toList $ compositeFields pdef
                               keyvals = map snd $ filter (\(a, _) -> let ret=isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ entityFields t) xs
                           in return keyvals
            case keyFromValues keyvals of
                Right k -> return k
                Left err -> error $ "selectKeysImpl: keyFromValues failed" <> show err

instance PersistUniqueWrite (SqlFor db) where
    upsertBy uniqueKey record updates = specializeQuery $ do
      conn <- ask
      let escape = connEscapeRawName conn
      let refCol n = Text.concat [escape (unEntityNameDB $ getEntityDBName t), ".", n]
      let mkUpdateText = mkUpdateText' (escape . unFieldNameDB) refCol
      case connUpsertSql conn of
        Just upsertSql -> case updates of
                            [] -> generalizeQuery $ defaultUpsertBy uniqueKey record updates
                            _:_ -> do
                                let upds = Text.intercalate "," $ map mkUpdateText updates
                                    sql = upsertSql t (NEL.fromList $ persistUniqueToFieldNames uniqueKey) upds
                                    vals = map toPersistValue (toPersistFields record)
                                        ++ map updatePersistValue updates
                                        ++ unqs uniqueKey

                                x <- rawSql sql vals
                                return $ head x
        Nothing -> generalizeQuery $ defaultUpsertBy uniqueKey record updates
        where
          t = entityDef $ Just record
          unqs uniqueKey' = concatMap persistUniqueToValues [uniqueKey']

    deleteBy uniq = specializeQuery $ do
        conn <- ask
        let sql' = sql conn
            vals = persistUniqueToValues uniq
        rawExecute sql' vals
      where
        t = entityDef $ dummyFromUnique uniq
        go = map snd . persistUniqueToFieldNames
        go' conn x = connEscapeRawName conn (unFieldNameDB x) `mappend` "=?"
        sql conn =
            Text.concat
                [ "DELETE FROM "
                , connEscapeRawName conn $ unEntityNameDB $ getEntityDBName t
                , " WHERE "
                , Text.intercalate " AND " $ map (go' conn) $ go uniq]

instance PersistUniqueRead (SqlFor a) where
    getBy uniq = specializeQuery $ do
        conn <- ask
        let sql =
                Text.concat
                    [ "SELECT "
                    , Text.intercalate "," $ dbColumns conn t
                    , " FROM "
                    , connEscapeRawName conn $ unEntityNameDB $ getEntityDBName t
                    , " WHERE "
                    , sqlClause conn]
            uvals = persistUniqueToValues uniq
        withRawQuery sql uvals $
            do row <- CL.head
               case row of
                   Nothing -> return Nothing
                   Just [] -> error "getBy: empty row"
                   Just vals ->
                       case parseEntityValues t vals of
                           Left err ->
                               liftIO $ throwIO $ PersistMarshalError err
                           Right r -> return $ Just r
      where
        sqlClause conn =
            Text.intercalate " AND " $ map (go conn . unFieldNameDB) $ toFieldNames' uniq
        go conn x = connEscapeRawName conn x `mappend` "=?"
        t = entityDef $ dummyFromUnique uniq
        toFieldNames' = map snd . persistUniqueToFieldNames

instance PersistQueryWrite (SqlFor db) where
    deleteWhere filts = do
        _ <- deleteWhereCount filts
        return ()
    updateWhere filts upds = do
        _ <- updateWhereCount filts upds
        return ()
    --
-- Here be dragons! These are functions, types, and helpers that were vendored
-- from Persistent.

-- | Same as 'deleteWhere', but returns the number of rows affected.
--
--
deleteWhereCount :: (PersistEntity val, MonadIO m, PersistEntityBackend val ~ SqlFor db)
                 => [Filter val]
                 -> ReaderT (SqlFor db) m Int64
deleteWhereCount filts = withReaderT unSqlFor $ do
    conn <- ask
    let t = entityDef $ dummyFromFilts filts
    let wher = if null filts
                then ""
                else filterClause False (SqlFor conn) filts
        sql = mconcat
            [ "DELETE FROM "
            , connEscapeRawName conn $ unEntityNameDB $ getEntityDBName t
            , wher
            ]
    rawExecuteCount sql $ getFiltsValues (SqlFor conn) filts

-- | Same as 'updateWhere', but returns the number of rows affected.
--
-- @since 1.1.5
updateWhereCount :: (PersistEntity val, MonadIO m, SqlFor db ~ PersistEntityBackend val)
                 => [Filter val]
                 -> [Update val]
                 -> ReaderT (SqlFor db) m Int64
updateWhereCount _ [] = return 0
updateWhereCount filts upds = withReaderT unSqlFor $ do
    conn <- ask
    let wher = if null filts
                then ""
                else filterClause False (SqlFor conn) filts
    let sql = mconcat
            [ "UPDATE "
            , connEscapeRawName conn $ unEntityNameDB $ getEntityDBName t
            , " SET "
            , Text.intercalate "," $ map (go' conn . go) upds
            , wher
            ]
    let dat = map updatePersistValue upds `Data.Monoid.mappend`
              getFiltsValues (SqlFor conn) filts
    rawExecuteCount sql dat
  where
    t = entityDef $ dummyFromFilts filts
    go'' n Assign = n <> "=?"
    go'' n Add = mconcat [n, "=", n, "+?"]
    go'' n Subtract = mconcat [n, "=", n, "-?"]
    go'' n Multiply = mconcat [n, "=", n, "*?"]
    go'' n Divide = mconcat [n, "=", n, "/?"]
    go'' _ (BackendSpecificUpdate up) = error $ Text.unpack $ "BackendSpecificUpdate" `mappend` up `mappend` "not supported"
    go' conn (x, pu) = go'' (connEscapeRawName conn x) pu
    go x = (updateField' x, updateUpdate x)

    updateField' (Update f _ _) = fieldName f
    updateField' _              = error "BackendUpdate not implemented"

dummyFromKey :: Key record -> Maybe record
dummyFromKey = Just . recordTypeFromKey

recordTypeFromKey :: Key record -> record
recordTypeFromKey _ = error "dummyFromKey"

whereStmtForKey :: PersistEntity record => SqlBackend -> Key record -> Text
whereStmtForKey conn k =
    Text.intercalate " AND "
  $ map (<> "=? ")
  $ NEL.toList $ dbIdColumns conn entDef
  where
    entDef = entityDef $ dummyFromKey k


insrepHelper :: (MonadIO m, PersistEntity val)
             => Text
             -> [Entity val]
             -> ReaderT SqlBackend m ()
insrepHelper _       []  = return ()
insrepHelper command es = do
    conn <- ask
    let columnNames = NEL.toList $ keyAndEntityColumnNames entDef conn
    rawExecute (sql conn columnNames) vals
  where
    entDef = entityDef $ map entityVal es
    sql conn columnNames = Text.concat
        [ command
        , " INTO "
        , connEscapeRawName conn (unEntityNameDB $ getEntityDBName entDef)
        , "("
        , Text.intercalate "," columnNames
        , ") VALUES ("
        , Text.intercalate "),(" $ replicate (length es) $ Text.intercalate "," $ map (const "?") columnNames
        , ")"
        ]
    vals = Foldable.foldMap entityValues es

data OrNull = OrNullYes | OrNullNo

filterClause :: (PersistEntity val, PersistEntityBackend val ~ SqlFor a)
             => Bool -- ^ include table name?
             -> SqlFor a
             -> [Filter val]
             -> Text
filterClause b c = fst . filterClauseHelper b True c OrNullNo

filterClauseHelper :: (PersistEntity val, PersistEntityBackend val ~ SqlFor a)
             => Bool -- ^ include table name?
             -> Bool -- ^ include WHERE?
             -> SqlFor a
             -> OrNull
             -> [Filter val]
             -> (Text, [PersistValue])
filterClauseHelper includeTable includeWhere (SqlFor conn) orNull filters =
    (if not (Text.null sql) && includeWhere
        then " WHERE " <> sql
        else sql, vals)
  where
    (sql, vals) = combineAND filters
    combineAND = combine " AND "

    combine s fs =
        (Text.intercalate s $ map wrapP a, mconcat b)
      where
        (a, b) = unzip $ map go fs
        wrapP x = Text.concat ["(", x, ")"]

    go (BackendFilter _) = error "BackendFilter not expected"
    go (FilterAnd []) = ("1=1", [])
    go (FilterAnd fs) = combineAND fs
    go (FilterOr []) = ("1=0", [])
    go (FilterOr fs)  = combine " OR " fs
    go (Filter field value pfilter) =
        let t = entityDef $ dummyFromFilts [Filter field value pfilter]
        in case (isIdField field, entityPrimary t, allVals) of
                 (True, Just pdef, PersistList ys:_) ->
                    if length (compositeFields pdef) /= length ys
                       then error $ "wrong number of entries in compositeFields vs PersistList allVals=" ++ show allVals
                    else
                      case (allVals, pfilter, isCompFilter pfilter) of
                        ([PersistList xs], Eq, _) ->
                           let sqlcl=Text.intercalate " and " (map (\a -> connEscapeRawName conn (unFieldNameDB $ fieldDB a) <> showSqlFilter pfilter <> "? ")  (NEL.toList $ compositeFields pdef))
                           in (wrapSql sqlcl,xs)
                        ([PersistList xs], Ne, _) ->
                           let sqlcl=Text.intercalate " or " (map (\a -> connEscapeRawName conn (unFieldNameDB $ fieldDB a) <> showSqlFilter pfilter <> "? ")  (NEL.toList $ compositeFields pdef))
                           in (wrapSql sqlcl,xs)
                        (_, In, _) ->
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(a,xs) -> connEscapeRawName conn (unFieldNameDB $ fieldDB a) <> showSqlFilter pfilter <> "(" <> Text.intercalate "," (replicate (length xs) " ?") <> ") ") (zip (NEL.toList $ compositeFields pdef) xxs)
                           in (wrapSql (Text.intercalate " and " (map wrapSql sqls)), concat xxs)
                        (_, NotIn, _) ->
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(a,xs) -> connEscapeRawName conn (unFieldNameDB $ fieldDB a) <> showSqlFilter pfilter <> "(" <> Text.intercalate "," (replicate (length xs) " ?") <> ") ") (zip (NEL.toList $ compositeFields pdef) xxs)
                           in (wrapSql (Text.intercalate " or " (map wrapSql sqls)), concat xxs)
                        ([PersistList xs], _, True) ->
                           let zs = tail (inits (NEL.toList $ compositeFields pdef))
                               sql1 = map (\b -> wrapSql (Text.intercalate " and " (map (\(i,a) -> sql2 (i==length b) a) (zip [1..] b)))) zs
                               sql2 islast a = connEscapeRawName conn (unFieldNameDB $ fieldDB a) <> (if islast then showSqlFilter pfilter else showSqlFilter Eq) <> "? "
                               sqlcl = Text.intercalate " or " sql1
                           in (wrapSql sqlcl, concat (tail (inits xs)))
                        (_, BackendSpecificFilter _, _) -> error "unhandled type BackendSpecificFilter for composite/non id primary keys"
                        _ -> error $ "unhandled type/filter for composite/non id primary keys pfilter=" ++ show pfilter ++ " persistList="++show allVals
                 (True, Just pdef, []) ->
                     error $ "empty list given as filter value filter=" ++ show pfilter ++ " persistList=" ++ show allVals ++ " pdef=" ++ show pdef
                 (True, Just pdef, _) ->
                     error $ "unhandled error for composite/non id primary keys filter=" ++ show pfilter ++ " persistList=" ++ show allVals ++ " pdef=" ++ show pdef

                 _ ->   case (isNull, pfilter, length notNullVals) of
                            (True, Eq, _) -> (name <> " IS NULL", [])
                            (True, Ne, _) -> (name <> " IS NOT NULL", [])
                            (False, Ne, _) -> (Text.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " <> "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            -- We use 1=2 (and below 1=1) to avoid using TRUE and FALSE, since
                            -- not all databases support those words directly.
                            (_, In, 0) -> ("1=2" <> orNullSuffix, [])
                            (False, In, _) -> (name <> " IN " <> qmarks <> orNullSuffix, allVals)
                            (True, In, _) -> (Text.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            (False, NotIn, 0) -> ("1=1", [])
                            (True, NotIn, 0) -> (name <> " IS NOT NULL", [])
                            (False, NotIn, _) -> (Text.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " NOT IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            (True, NotIn, _) -> (Text.concat
                                [ "("
                                , name
                                , " IS NOT NULL AND "
                                , name
                                , " NOT IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            _ -> (name <> showSqlFilter pfilter <> "?" <> orNullSuffix, allVals)

      where
        isCompFilter Lt = True
        isCompFilter Le = True
        isCompFilter Gt = True
        isCompFilter Ge = True
        isCompFilter _  =  False

        wrapSql sqlcl = "(" <> sqlcl <> ")"
        fromPersistList (PersistList xs) = xs
        fromPersistList other = error $ "expected PersistList but found " ++ show other

        filterValueToPersistValues :: forall a.  PersistField a => FilterValue a -> [PersistValue]
        filterValueToPersistValues v = case v of
            FilterValue a   -> map toPersistValue [a]
            FilterValues as -> map toPersistValue as
            UnsafeValue a   -> map toPersistValue [a]

        orNullSuffix =
            case orNull of
                OrNullYes -> mconcat [" OR ", name, " IS NULL"]
                OrNullNo  -> ""

        isNull = PersistNull `elem` allVals
        notNullVals = filter (/= PersistNull) allVals
        allVals = filterValueToPersistValues value
        tn = connEscapeRawName conn $ unEntityNameDB $ getEntityDBName
           $ entityDef $ dummyFromFilts [Filter field value pfilter]
        name =
            (if includeTable
                then ((tn <> ".") <>)
                else id)
            $ connEscapeRawName conn $ fieldName field
        qmarks = case value of
                    FilterValues x ->
                        let x' = filter (/= PersistNull) $ map toPersistValue x
                         in "(" <> Text.intercalate "," (map (const "?") x') <> ")"
                    _ -> "?"
        showSqlFilter Eq                        = "="
        showSqlFilter Ne                        = "<>"
        showSqlFilter Gt                        = ">"
        showSqlFilter Lt                        = "<"
        showSqlFilter Ge                        = ">="
        showSqlFilter Le                        = "<="
        showSqlFilter In                        = " IN "
        showSqlFilter NotIn                     = " NOT IN "
        showSqlFilter (BackendSpecificFilter s) = s

dummyFromFilts :: [Filter v] -> Maybe v
dummyFromFilts _ = Nothing

fieldName ::  forall record typ a.  (PersistEntity record, PersistEntityBackend record ~ SqlFor a) => EntityField record typ -> Text
fieldName f = unFieldNameDB $ fieldDB $ persistFieldDef f


getFiltsValues :: forall val a. (PersistEntity val, PersistEntityBackend val ~ SqlFor a)
               => SqlFor a -> [Filter val] -> [PersistValue]
getFiltsValues conn = snd . filterClauseHelper False False conn OrNullNo

orderClause :: (PersistEntity val, PersistEntityBackend val ~ SqlFor a)
            => Bool -- ^ include the table name
            -> SqlFor a
            -> SelectOpt val
            -> Text
orderClause includeTable (SqlFor conn) o =
    case o of
        Asc  x -> name x
        Desc x -> name x <> " DESC"
        _      -> error "orderClause: expected Asc or Desc, not limit or offset"
  where
    dummyFromOrder :: SelectOpt a -> Maybe a
    dummyFromOrder _ = Nothing

    tn = connEscapeRawName conn $ unEntityNameDB $ getEntityDBName $ entityDef $ dummyFromOrder o

    name :: (PersistEntityBackend record ~ SqlFor a, PersistEntity record)
         => EntityField record typ -> Text
    name x =
        (if includeTable
            then ((tn <> ".") <>)
            else id)
        $ connEscapeRawName conn $ fieldName x

dummyFromUnique :: Unique v -> Maybe v
dummyFromUnique _ = Nothing

-- escape :: DBName -> Text.Text
-- escape (DBName s) = Text.pack $ '"' : escapeQuote (Text.unpack s) ++ "\""
--   where
--     escapeQuote ""       = ""
--     escapeQuote ('"':xs) = "\"\"" ++ escapeQuote xs
--     escapeQuote (x:xs)   = x : escapeQuote xs

runChunked
    :: (Monad m)
    => Int
    -> ([a] -> ReaderT SqlBackend m ())
    -> [a]
    -> ReaderT SqlBackend m ()
runChunked _ _ []     = return ()
runChunked width m xs = do
    conn <- ask
    case connMaxParams conn of
        Nothing -> m xs
        Just maxParams -> let chunkSize = maxParams `div` width in
            mapM_ m (chunksOf chunkSize xs)

-- Implement this here to avoid depending on the split package
chunksOf :: Int -> [a] -> [[a]]
chunksOf _ [] = []
chunksOf size xs = let (chunk, rest) = splitAt size xs in chunk : chunksOf size rest

-- | The slow but generic 'upsertBy' implementation for any 'PersistUniqueRead'.
-- * Lookup corresponding entities (if any) 'getBy'.
-- * If the record exists, update using 'updateGet'.
-- * If it does not exist, insert using 'insertEntity'.
-- @since 2.11
defaultUpsertBy
    :: ( PersistEntityBackend record ~ backend
       , PersistEntity record
       , BaseBackend backend ~ backend
       , BackendCompatible SqlBackend backend
       , MonadIO m
       , PersistStoreWrite backend
       , PersistUniqueRead backend
       )
    => Unique record   -- ^ uniqueness constraint to find by
    -> record          -- ^ new record to insert
    -> [Update record] -- ^ updates to perform if the record already exists
    -> ReaderT backend m (Entity record) -- ^ the record in the database after the operation
defaultUpsertBy uniqueKey record updates = do
    mrecord <- getBy uniqueKey
    maybe (insertEntity record) (`updateGetEntity` updates) mrecord
  where
    updateGetEntity (Entity k _) upds =
        (Entity k) `fmap` (updateGet k upds)
