{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

module EsqueletoSpec where

import Database.Persist.Typed
import Database.Esqueleto
import Database.Persist.TH
import Test.Hspec

data TestDb

share [mkPersist (mkSqlSettingsFor ''TestDb)] [persistLowerCase|

Person
    name String
    age Int
    deriving Show Eq

Dog
    name String
    owner PersonId
    deriving Show Eq

Foo
    Id sql=other_id
    other_id Int
    |]

spec :: Spec
spec = do
    let typeChecks = True `shouldBe` True
    describe "select" $
        it "type checks" $ do
            let q :: SqlPersistMFor TestDb [(Entity Person, Entity Dog)]
                q = select $
                    from $ \(p `InnerJoin` d) -> do
                    on (p ^. PersonId ==. d ^. DogOwner)
                    pure (p, d)
            typeChecks

    describe "update" $
        it "type checks" $ do
            let q :: SqlPersistMFor TestDb ()
                q = update $ \p -> do
                    set p [ PersonName =. val "world" ]
                    where_ (p ^. PersonName ==. val "hello")
            typeChecks

    describe "delete" $
        it "type checks" $ do
            let q :: SqlPersistMFor TestDb ()
                q = delete $ from $ \p -> where_ (p ^. PersonName ==. val "world")
            typeChecks

    describe "issue #2" $ do
        it "type checks" $ do
            let k = toSqlKeyFor 3 :: Key Foo
            typeChecks
