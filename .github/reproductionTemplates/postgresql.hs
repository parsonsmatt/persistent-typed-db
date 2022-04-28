#!/usr/bin/env stack
-- stack --resolver lts-9.21 script
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
import Control.Monad.Logger (runStdoutLoggingT)
import Control.Monad.IO.Class  (liftIO)
import Database.Persist
import Database.Persist.Postgresql
import Database.Persist.TH
import Control.Monad.Reader

share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
Person
    name String
    age Int Maybe
    deriving Show Eq
BlogPost
    title String
    authorId PersonId
    deriving Show Eq
|]

main :: IO ()
main = runStdoutLoggingT $ withPostgresqlPool "dbname=persistent" 1 $ liftSqlPersistMPool $ do
  runMigration migrateAll

  johnId <- insert $ Person "John Doe" $ Just 35
  delete johnId
