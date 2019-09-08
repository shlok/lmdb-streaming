--------------------------------------------------------------------------------

{-# LANGUAGE TypeApplications #-}

--------------------------------------------------------------------------------

module Database.LMDB.Streaming.Tests (tests) where

--------------------------------------------------------------------------------

import Control.Concurrent.Async (asyncBound, wait)
import Control.Monad (forM_)
import Control.Monad.Trans.Resource (runResourceT)
import Data.ByteString (ByteString, pack)
import Data.List (foldl', nubBy, sort)
import Database.LMDB.Raw (MDB_dbi', MDB_env, mdb_clear', mdb_put', mdb_txn_begin, mdb_txn_commit)
import Database.LMDB.Resource.Utility (emptyWriteFlags, marshalOut)
import Database.LMDB.Streaming (readLMDB, writeLMDB)
import Streaming.Prelude (each, toList_)
import qualified Streaming.Prelude as S (take)
import Test.QuickCheck.Monadic (PropertyM, monadicIO, pick, run)
import Test.Tasty (TestTree)
import Test.Tasty.QuickCheck (arbitrary, testProperty)
import UnliftIO.Exception (tryAny)

--------------------------------------------------------------------------------

tests :: IO (MDB_env, MDB_dbi') -> [TestTree]
tests res = [ testReadLMDB res, testWriteLMDB res, testWriteLMDB2 res ]

--------------------------------------------------------------------------------

-- | Clear the database, write key-value pairs to it in a normal manner, read
-- them back using our library, and make sure the result is what we wrote.
testReadLMDB :: IO (MDB_env, MDB_dbi') -> TestTree
testReadLMDB res = testProperty "readLMDB" . monadicIO $ do
    (env, dbi) <- run res
    run $ clearDB env dbi
    keyValuePairs <- arbitraryKeyValuePairs
    run $ (asyncBound $ do
        txn <- mdb_txn_begin env Nothing False
        forM_ keyValuePairs $ \(k, v) -> marshalOut k $ \k' ->
                                         marshalOut v $ \v' -> mdb_put' emptyWriteFlags txn dbi k' v' >> return ()
        mdb_txn_commit txn) >>= wait
    readPairsAll <- run . runResourceT . toList_ $ readLMDB env dbi
    let allAsExpected = readPairsAll == (sort . removeDuplicateKeys $ keyValuePairs)
    readPairsFirstFew <- run . runResourceT . toList_ . S.take 3 $ readLMDB env dbi
    let firstFewAsExpected = readPairsFirstFew == (take 3 . sort . removeDuplicateKeys $ keyValuePairs)
    return $ allAsExpected && firstFewAsExpected

-- | Clear the database, write key-value pairs to it using our library with key overwriting allowed, read
-- them back using our library (already covered by 'testReadLMDB'), and make sure the result is what we wrote.
testWriteLMDB :: IO (MDB_env, MDB_dbi') -> TestTree
testWriteLMDB res = testProperty "writeLMDB" . monadicIO $ do
    (env, dbi) <- run res
    run $ clearDB env dbi
    keyValuePairs <- arbitraryKeyValuePairs
    retValue <- pick (arbitrary @String)
    r <- run . runResourceT $ writeLMDB env dbi False (each keyValuePairs >> return retValue)
    readPairs <- run . runResourceT . toList_ $ readLMDB env dbi
    return $ r == retValue && readPairs == (sort . removeDuplicateKeys $ keyValuePairs)

-- | Clear the database, write key-value pairs to it using our library with key overwriting
-- disallowed, and make sure an exception occurs iff we had duplicate keys in our pairs.
testWriteLMDB2 :: IO (MDB_env, MDB_dbi') -> TestTree
testWriteLMDB2 res = testProperty "writeLMDB2" . monadicIO $ do
    (env, dbi) <- run res
    run $ clearDB env dbi
    keyValuePairs <- arbitraryKeyValuePairs
    e <- run . tryAny . runResourceT $ writeLMDB env dbi True (each keyValuePairs)
    case e of
        Left _ -> return $ hasDuplicateKeys keyValuePairs
        Right _ -> return . not $ hasDuplicateKeys keyValuePairs

clearDB :: MDB_env -> MDB_dbi' -> IO ()
clearDB env dbi = (asyncBound $ do
    txn <- mdb_txn_begin env Nothing False
    mdb_clear' txn dbi
    mdb_txn_commit txn) >>= wait

arbitraryKeyValuePairs :: PropertyM IO [(ByteString, ByteString)]
arbitraryKeyValuePairs =
    map (\(ws1, ws2) -> (pack ws1, pack ws2))
    . filter (\(ws1, _) -> length ws1 > 0) -- LMDB does not allow empty keys.
   <$> pick arbitrary

-- | Note that this function retains the last value for each key.
removeDuplicateKeys :: (Eq a) => [(a, b)] -> [(a, b)]
removeDuplicateKeys = foldl' (\acc (a, b) -> if any ((== a) . fst) acc then acc else (a, b) : acc) [] . reverse

hasDuplicateKeys :: (Eq a) => [(a, b)] -> Bool
hasDuplicateKeys l =
    let l2 = nubBy (\(a1, _) (a2, _) -> a1 == a2) l
     in length l /= length l2

--------------------------------------------------------------------------------
