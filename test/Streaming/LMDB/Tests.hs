--------------------------------------------------------------------------------

module Streaming.LMDB.Tests (tests) where

--------------------------------------------------------------------------------

import Control.Concurrent.Async (asyncBound, wait)
import Control.Monad (forM_)
import Control.Monad.Trans.Resource (runResourceT)
import Data.ByteString (pack)
import Data.List (sort)
import qualified Data.Map.Strict as M (fromList, toList)
import Database.LMDB.Raw (MDB_dbi', MDB_env, mdb_clear', mdb_put', mdb_txn_begin, mdb_txn_commit)
import Streaming.LMDB (readLMDB, writeLMDB)
import Streaming.LMDB.Internal (marshalOut, noWriteFlags)
import Streaming.Prelude (each, toList_)
import qualified Streaming.Prelude as S (take)
import Test.QuickCheck.Monadic (monadicIO, pick, run)
import Test.Tasty (TestTree)
import Test.Tasty.QuickCheck (arbitrary, testProperty)

--------------------------------------------------------------------------------

tests :: IO (MDB_env, MDB_dbi') -> [TestTree]
tests res = [ testReadLMDB res, testWriteLMDB res ]

--------------------------------------------------------------------------------

-- | Clear the database, write key-value pairs to it in a normal manner, read them
-- back using our streaming functionality, and make sure the result is what we wrote.
testReadLMDB :: IO (MDB_env, MDB_dbi') -> TestTree
testReadLMDB res = testProperty "readLMDB" . monadicIO $ do
    (env, dbi) <- run res
    keyValuePairs <- removeDuplicateKeys . map (\(ws1, ws2) -> (pack ws1, pack ws2))
                                         . filter (\(ws1, _) -> length ws1 > 0) -- LMDB does not allow empty keys.
                                        <$> pick arbitrary
    run $ (asyncBound $ do
        txn <- mdb_txn_begin env Nothing False
        mdb_clear' txn dbi
        forM_ keyValuePairs $ \(k, v) -> marshalOut k $ \k' ->
                                         marshalOut v $ \v' -> mdb_put' noWriteFlags txn dbi k' v' >> return ()
        mdb_txn_commit txn) >>= wait
    readPairsAll <- run . runResourceT . toList_ $ readLMDB env dbi
    let allAsExpected = readPairsAll == sort keyValuePairs
    readPairsFirstFew <- run . runResourceT . toList_ . S.take 3 $ readLMDB env dbi
    let firstFewAsExpected = readPairsFirstFew == (take 3 $ sort keyValuePairs)
    return $ allAsExpected && firstFewAsExpected

removeDuplicateKeys :: (Ord a) => [(a, b)] -> [(a, b)]
removeDuplicateKeys = M.toList . M.fromList

--------------------------------------------------------------------------------

-- | Clear the database, write key-value pairs to it using our streaming functionality,
-- read them back using our streaming functionality (which 'testReadLMDB' already tests),
-- and make sure the result is what we wrote.
testWriteLMDB :: IO (MDB_env, MDB_dbi') -> TestTree
testWriteLMDB res = testProperty "writeLMDB" . monadicIO $ do
    (env, dbi) <- run res
    run $ (asyncBound $ do
        txn <- mdb_txn_begin env Nothing False
        mdb_clear' txn dbi
        mdb_txn_commit txn) >>= wait
    keyValuePairs <- removeDuplicateKeys . map (\(ws1, ws2) -> (pack ws1, pack ws2))
                                         . filter (\(ws1, _) -> length ws1 > 0) -- LMDB does not allow empty keys.
                                        <$> pick arbitrary
    run . runResourceT . writeLMDB env dbi $ each keyValuePairs
    readPairs <- run . runResourceT . toList_ $ readLMDB env dbi
    return $ readPairs == sort keyValuePairs -- We expect to get the keys back in sorted order.

--------------------------------------------------------------------------------
