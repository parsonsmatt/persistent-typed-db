{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

module Database.Persist.Typed where

import           Control.Exception
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Reader
import           Control.Monad.Trans.Resource
import           Control.Monad.Logger
import           Data.Aeson                          as A
import           Data.ByteString.Char8               (readInteger)
import           Data.Coerce                         (coerce)
import           Data.Conduit
import qualified Data.Conduit.List                   as CL
import           Data.Int
import           Data.List                           (find, inits, transpose)
import           Data.Maybe                          (isJust)
import           Data.Monoid
import           Data.Pool                           (Pool)
import           Data.Text                           (Text)
import qualified Data.Text                           as Text
import           Database.Persist
import           Database.Persist.Sql                hiding (deleteWhereCount,
                                                      updateWhereCount)
import           Database.Persist.Sql.Types.Internal
import           Database.Persist.Sql.Util           (dbColumns, dbIdColumns,
                                                      entityColumnNames,
                                                      isIdField,
                                                      keyAndEntityColumnNames,
                                                      parseEntityValues)
import           Database.Persist.TH
import           Language.Haskell.TH
import           Web.HttpApiData
import           Web.PathPieces

-- | A wrapper around 'SqlBackend' type. To specialize this to a specific
-- database, fill in the type parameter.
newtype SqlFor a = SqlFor { unSqlFor :: SqlBackend }

-- | 'AnySql' refers to general SQL queries that can be used across any
-- database.
type AnySql = forall a. SqlFor a

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
type SqlPersistTFor db = ReaderT (SqlFor db)

-- | A 'Pool' of database connections that are specialized to a specific
-- database.
type ConnectionPoolFor db = Pool (SqlFor db)
--
-- | A specialization of 'SqlPersistM' that uses the underlying @db@ database
-- type.
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
specializeQuery :: SqlPersistT m a -> SqlPersistTFor db m a
specializeQuery = withReaderT unSqlFor

-- | Generalizes a query from a specific database
generalizeQuery :: SqlPersistTFor db m a -> SqlPersistT m a
generalizeQuery = withReaderT SqlFor

-- | Use the 'SqlFor' type for the database connection backend.
mkSqlSettingsFor :: Name -> MkPersistSettings
mkSqlSettingsFor n = mkPersistSettings (AppT (ConT ''SqlFor) (ConT n))

-- | Persistent's @toSqlKey@ and @fromSqlKey@ hardcode the 'SqlBackend', so we
-- have to reimplement them here.
toSqlKey :: (ToBackendKey (SqlFor a) record) => Int64 -> Key record
toSqlKey = fromBackendKey . SqlForKey . SqlBackendKey

-- | Persistent's @toSqlKey@ and @fromSqlKey@ hardcode the 'SqlBackend', so we
-- have to reimplement them here.
fromSqlKey :: ToBackendKey (SqlFor a) record => Key record -> Int64
fromSqlKey = unSqlBackendKey . unSqlForKey . toBackendKey

-- | Specialize a 'ConnectionPool' to a @'Pool' ('SqlFor' db)@. You should apply
-- this whenever you create or initialize the database connection pooling to
-- avoid potentially mixing the database pools up.
specializePool :: ConnectionPool -> ConnectionPoolFor db
specializePool = coerce

-- | Generalize a @'Pool' ('SqlFor' db)@ to an ordinary 'ConnectionPool'. This
-- renders the pool unusable for model-specific code that relies on the type
-- safety, but allows you to use it for general-purpose SQL queries.
generalizePool :: ConnectionPoolFor db -> ConnectionPool
generalizePool = coerce

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
                 $ map (connEscapeName conn . fieldDB) $ entityFields t
            noColumns :: Bool
            noColumns = null $ entityFields t
        let wher = whereStmtForKey conn k
        let sql = Text.concat
                [ "SELECT "
                , if noColumns then "*" else cols
                , " FROM "
                , connEscapeName conn $ entityDB t
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
        let go' (x, pu) = go'' (connEscapeName conn x) pu
        let wher = whereStmtForKey conn k
        let sql = Text.concat
                [ "UPDATE "
                , connEscapeName conn $ tableDBName $ recordTypeFromKey k
                , " SET "
                , Text.intercalate "," $ map (go' . go) upds
                , " WHERE "
                , wher
                ]
        rawExecute sql $
            map updatePersistValue upds `mappend` keyToValues k
      where
        go x = (fieldDB $ updateFieldDef x, updateUpdate x)

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
                        let pks = map fieldHaskell $ compositeFields pdef
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
                  , connEscapeName conn (entityDB t)
                  , "("
                  , Text.intercalate "," $ map (connEscapeName conn . fieldDB) $ entityFields t
                  , ") VALUES ("
                  , Text.intercalate "),(" $ replicate (length valss) $ Text.intercalate "," $ map (const "?") (entityFields t)
                  , ")"
                  ]
          rawExecute sql (concat valss)

        t = entityDef vals0
        -- Implement this here to avoid depending on the split package
        chunksOf _ [] = []
        chunksOf size xs = let (chunk, rest) = splitAt size xs in chunk : chunksOf size rest

    replace k val = do
        conn <- asks unSqlFor
        let t = entityDef $ Just val
        let wher = whereStmtForKey conn k
        let sql = Text.concat
                [ "UPDATE "
                , connEscapeName conn (entityDB t)
                , " SET "
                , Text.intercalate "," (map (go conn . fieldDB) $ entityFields t)
                , " WHERE "
                , wher
                ]
            vals = map toPersistValue (toPersistFields val) `mappend` keyToValues k
        specializeQuery $ rawExecute sql vals
      where
        go conn x = connEscapeName conn x `Text.append` "=?"

    insertKey k = specializeQuery . insrepHelper "INSERT" k

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
            , connEscapeName conn $ tableDBName $ recordTypeFromKey k
            , " WHERE "
            , wher conn
            ]

-- orphaned instance for convenience of modularity
instance PersistQueryRead (SqlFor a) where
    count filts = specializeQuery $ do
        conn <- ask
        let wher = if null filts
                    then ""
                    else filterClause False (SqlFor conn) filts
        let sql = mconcat
                [ "SELECT COUNT(*) FROM "
                , connEscapeName conn $ entityDB t
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
        return $ fmap ($= CL.mapM parse) srcRes
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
        sql conn = connLimitOffset conn (limit,offset) (not (null orders)) $ mconcat
            [ "SELECT "
            , cols conn
            , " FROM "
            , connEscapeName conn $ entityDB t
            , wher conn
            , ord (SqlFor conn)
            ]

    selectKeysRes filts opts = specializeQuery $ do
        conn <- ask
        srcRes <- rawQueryRes (sql conn) (getFiltsValues (SqlFor conn) filts)
        return $ fmap ($= CL.mapM parse) srcRes
      where
        t = entityDef $ dummyFromFilts filts
        cols conn = Text.intercalate "," $ dbIdColumns conn t


        wher conn = if null filts
                    then ""
                    else filterClause False (SqlFor conn) filts
        sql conn = connLimitOffset conn (limit,offset) (not (null orders)) $ mconcat
            [ "SELECT "
            , cols conn
            , " FROM "
            , connEscapeName conn $ entityDB t
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
                           let pks = map fieldHaskell $ compositeFields pdef
                               keyvals = map snd $ filter (\(a, _) -> let ret=isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ entityFields t) xs
                           in return keyvals
            case keyFromValues keyvals of
                Right k -> return k
                Left err -> error $ "selectKeysImpl: keyFromValues failed" <> show err

instance PersistUniqueWrite (SqlFor db) where
    upsert record updates = specializeQuery $ do
      conn <- ask
      uniqueKey <- withReaderT SqlFor $ onlyUnique record
      case connUpsertSql conn of
        Just upsertSql -> case updates of
                            [] -> withReaderT SqlFor $ defaultUpsert record updates
                            _:_ -> do
                                let upds = Text.intercalate "," $ map (go' . go) updates
                                    sql = upsertSql t upds
                                    vals = map toPersistValue (toPersistFields record)
                                        ++ map updatePersistValue updates
                                        ++ unqs uniqueKey

                                    go'' n Assign = n <> "=?"
                                    go'' n Add = Text.concat [n, "=", escape (entityDB t) <> ".", n, "+?"]
                                    go'' n Subtract = Text.concat [n, "=", escape (entityDB t) <> ".", n, "-?"]
                                    go'' n Multiply = Text.concat [n, "=", escape (entityDB t) <> ".", n, "*?"]
                                    go'' n Divide = Text.concat [n, "=", escape (entityDB t) <> ".", n, "/?"]
                                    go'' _ (BackendSpecificUpdate up) = error $ Text.unpack $ "BackendSpecificUpdate" `Data.Monoid.mappend` up `mappend` "not supported"

                                    go' (x, pu) = go'' (connEscapeName conn x) pu
                                    go x = (fieldDB $ updateFieldDef x, updateUpdate x)

                                x <- rawSql sql vals
                                return $ head x
        Nothing -> withReaderT SqlFor $ defaultUpsert record updates
        where
          t = entityDef $ Just record
          unqs uniqueKey = concatMap persistUniqueToValues [uniqueKey]

    deleteBy uniq = specializeQuery $ do
        conn <- ask
        let sql' = sql conn
            vals = persistUniqueToValues uniq
        rawExecute sql' vals
      where
        t = entityDef $ dummyFromUnique uniq
        go = map snd . persistUniqueToFieldNames
        go' conn x = connEscapeName conn x `mappend` "=?"
        sql conn =
            Text.concat
                [ "DELETE FROM "
                , connEscapeName conn $ entityDB t
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
                    , connEscapeName conn $ entityDB t
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
            Text.intercalate " AND " $ map (go conn) $ toFieldNames' uniq
        go conn x = connEscapeName conn x `mappend` "=?"
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
            , connEscapeName conn $ entityDB t
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
            , connEscapeName conn $ entityDB t
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
    go' conn (x, pu) = go'' (connEscapeName conn x) pu
    go x = (updateField x, updateUpdate x)

    updateField (Update f _ _) = fieldName f
    updateField _              = error "BackendUpdate not implemented"

dummyFromKey :: Key record -> Maybe record
dummyFromKey = Just . recordTypeFromKey

recordTypeFromKey :: Key record -> record
recordTypeFromKey _ = error "dummyFromKey"

whereStmtForKey :: PersistEntity record => SqlBackend -> Key record -> Text
whereStmtForKey conn k =
    Text.intercalate " AND "
  $ map (<> "=? ")
  $ dbIdColumns conn entDef
  where
    entDef = entityDef $ dummyFromKey k


insrepHelper :: (MonadIO m, PersistEntity val)
             => Text
             -> Key val
             -> val
             -> ReaderT SqlBackend m ()
insrepHelper command k record = do
    conn <- ask
    let columnNames = keyAndEntityColumnNames entDef conn
    rawExecute (sql conn columnNames) vals
  where
    entDef = entityDef $ Just record
    sql conn columnNames = Text.concat
        [ command
        , " INTO "
        , connEscapeName conn (entityDB entDef)
        , "("
        , Text.intercalate "," columnNames
        , ") VALUES("
        , Text.intercalate "," (map (const "?") columnNames)
        , ")"
        ]
    vals = entityValues (Entity k record)

updateFieldDef :: PersistEntity v => Update v -> FieldDef
updateFieldDef (Update f _ _) =
    persistFieldDef f
updateFieldDef BackendUpdate {} =
    error "updateFieldDef did not expect BackendUpdate"


updatePersistValue :: Update v -> PersistValue
updatePersistValue (Update _ v _) =
    toPersistValue v
updatePersistValue BackendUpdate {} =
    error "updatePersistValue did not expect BackendUpdate"

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
                           let sqlcl=Text.intercalate " and " (map (\a -> connEscapeName conn (fieldDB a) <> showSqlFilter pfilter <> "? ")  (compositeFields pdef))
                           in (wrapSql sqlcl,xs)
                        ([PersistList xs], Ne, _) ->
                           let sqlcl=Text.intercalate " or " (map (\a -> connEscapeName conn (fieldDB a) <> showSqlFilter pfilter <> "? ")  (compositeFields pdef))
                           in (wrapSql sqlcl,xs)
                        (_, In, _) ->
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(a,xs) -> connEscapeName conn (fieldDB a) <> showSqlFilter pfilter <> "(" <> Text.intercalate "," (replicate (length xs) " ?") <> ") ") (zip (compositeFields pdef) xxs)
                           in (wrapSql (Text.intercalate " and " (map wrapSql sqls)), concat xxs)
                        (_, NotIn, _) ->
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(a,xs) -> connEscapeName conn (fieldDB a) <> showSqlFilter pfilter <> "(" <> Text.intercalate "," (replicate (length xs) " ?") <> ") ") (zip (compositeFields pdef) xxs)
                           in (wrapSql (Text.intercalate " or " (map wrapSql sqls)), concat xxs)
                        ([PersistList xs], _, True) ->
                           let zs = tail (inits (compositeFields pdef))
                               sql1 = map (\b -> wrapSql (Text.intercalate " and " (map (\(i,a) -> sql2 (i==length b) a) (zip [1..] b)))) zs
                               sql2 islast a = connEscapeName conn (fieldDB a) <> (if islast then showSqlFilter pfilter else showSqlFilter Eq) <> "? "
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

        filterValueToPersistValues :: forall a.  PersistField a => Either a [a] -> [PersistValue]
        filterValueToPersistValues v = map toPersistValue $ either return id v

        orNullSuffix =
            case orNull of
                OrNullYes -> mconcat [" OR ", name, " IS NULL"]
                OrNullNo  -> ""

        isNull = PersistNull `elem` allVals
        notNullVals = filter (/= PersistNull) allVals
        allVals = filterValueToPersistValues value
        tn = connEscapeName conn $ entityDB
           $ entityDef $ dummyFromFilts [Filter field value pfilter]
        name =
            (if includeTable
                then ((tn <> ".") <>)
                else id)
            $ connEscapeName conn $ fieldName field
        qmarks = case value of
                    Left _ -> "?"
                    Right x ->
                        let x' = filter (/= PersistNull) $ map toPersistValue x
                         in "(" <> Text.intercalate "," (map (const "?") x') <> ")"
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

fieldName ::  forall record typ a.  (PersistEntity record, PersistEntityBackend record ~ SqlFor a) => EntityField record typ -> DBName
fieldName f = fieldDB $ persistFieldDef f


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
        _ -> error "orderClause: expected Asc or Desc, not limit or offset"
  where
    dummyFromOrder :: SelectOpt a -> Maybe a
    dummyFromOrder _ = Nothing

    tn = connEscapeName conn $ entityDB $ entityDef $ dummyFromOrder o

    name :: (PersistEntityBackend record ~ SqlFor a, PersistEntity record)
         => EntityField record typ -> Text
    name x =
        (if includeTable
            then ((tn <> ".") <>)
            else id)
        $ connEscapeName conn $ fieldName x

defaultUpsert
    :: (MonadIO m
       ,PersistEntity record
       ,PersistUniqueWrite backend
       ,PersistEntityBackend record ~ BaseBackend backend)
    => record -> [Update record] -> ReaderT backend m (Entity record)
defaultUpsert record updates = do
    uniqueKey <- onlyUnique record
    upsertBy uniqueKey record updates

dummyFromUnique :: Unique v -> Maybe v
dummyFromUnique _ = Nothing

escape :: DBName -> Text.Text
escape (DBName s) = Text.pack $ '"' : escapeQuote (Text.unpack s) ++ "\""
  where
    escapeQuote ""       = ""
    escapeQuote ('"':xs) = "\"\"" ++ escapeQuote xs
    escapeQuote (x:xs)   = x : escapeQuote xs
