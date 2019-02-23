--------------------------------------------------------------------------------

module Streaming.LMDB (readLMDB, writeLMDB) where

--------------------------------------------------------------------------------

import Control.Concurrent.Async (asyncBound)
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Concurrent.MVar (MVar, isEmptyMVar, newEmptyMVar, putMVar, takeMVar)
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Resource (MonadResource, allocate, register, release, unprotect)
import Data.ByteString (ByteString)
import Database.LMDB.Raw (MDB_cursor', MDB_cursor_op (MDB_FIRST, MDB_NEXT), MDB_dbi',
                          MDB_env, MDB_txn, MDB_val, mdb_cursor_close', mdb_cursor_get',
                          mdb_cursor_open', mdb_put', mdb_txn_abort, mdb_txn_begin, mdb_txn_commit)
import Foreign (Ptr, free, malloc, peek)
import Streaming.LMDB.Internal (marshalIn, marshalOut, noWriteFlags)
import Streaming.Prelude (Of, Stream, snd', yield)
import qualified Streaming.Prelude as S (foldM)

--------------------------------------------------------------------------------

-- | A read transaction is kept open for the duration of the stream.
-- Bear in mind LMDB's caveats regarding long-lived transactions.
readLMDB :: (MonadResource m) => MDB_env -> MDB_dbi' -> Stream (Of (ByteString, ByteString)) m ()
readLMDB env dbi = do
    (txnKey, txn) <- lift $ allocate (mdb_txn_begin env Nothing True) mdb_txn_abort
    (cursKey, curs) <- lift $ allocate (mdb_cursor_open' txn dbi) mdb_cursor_close'
    (ptrKey, (kp, vp)) <- lift $ allocate ((,) <$> malloc <*> malloc) (\(kp, vp) -> free kp >> free vp)
    yieldAll curs kp vp True
    _ <- release ptrKey >> release cursKey >> unprotect txnKey
    liftIO $ mdb_txn_commit txn

yieldAll :: (MonadIO m) => MDB_cursor' -> Ptr MDB_val -> Ptr MDB_val
         -> Bool -> Stream (Of (ByteString, ByteString)) m ()
yieldAll curs kp vp first = do
    found <- liftIO $ mdb_cursor_get' (if first then MDB_FIRST else MDB_NEXT) curs kp vp
    when found $ do
        k <- liftIO (peek kp >>= marshalIn)
        v <- liftIO (peek vp >>= marshalIn)
        yield (k, v)
        yieldAll curs kp vp False

--------------------------------------------------------------------------------

-- | A write transaction is kept open for the duration of the stream.
-- Bear in mind LMDB's caveats regarding long-lived transactions.
writeLMDB :: (MonadResource m) => MDB_env -> MDB_dbi' -> Stream (Of (ByteString, ByteString)) m r -> m r
writeLMDB env dbi stream = do
    chan' <- liftIO newChan
    resultMVar' <- liftIO newEmptyMVar
    finishMVar' <- liftIO newEmptyMVar
    let channel = Channel { chan = chan', resultMVar = resultMVar', finishMVar = finishMVar' }
    _ <- liftIO . asyncBound $ startChannel channel
    finishKey <- register $ endChannel channel
    (txnKey, txn) <- allocate (runTxn channel $ mdb_txn_begin env Nothing False)
                              (\txn -> runEmpty channel $ mdb_txn_abort txn)
    let λ _ (k, v) = liftIO . runEmpty channel . marshalOut k $ \k' -> marshalOut v $ \v' ->
            mdb_put' noWriteFlags txn dbi k' v' >> return ()
    r <- S.foldM λ (return ()) return stream
    _ <- unprotect txnKey
    liftIO . runEmpty channel $ mdb_txn_commit txn
    release finishKey
    return $ snd' r

-- LMDB requires write transactions to happen on a bound thread.
-- The following machinery helps us with that.

data Result = ResultTxn MDB_txn | ResultEmpty ()

data Channel =
     Channel { chan       :: !(Chan (IO Result))
             , resultMVar :: !(MVar Result)
             , finishMVar :: !(MVar ()) }

-- | We will call this with asyncBound, which will make sure that
-- IO actions that are written to the Chan are run on a bound thread.
startChannel :: Channel -> IO ()
startChannel channel@(Channel { chan = chan', resultMVar = resultMVar', finishMVar = finishMVar' }) = do
    io <- readChan chan'
    result <- io
    putMVar resultMVar' result
    isEmptyMVar finishMVar' >>= (flip when) (startChannel channel)

runTxn :: Channel -> IO MDB_txn -> IO MDB_txn
runTxn (Channel { chan = chan', resultMVar = resultMVar' }) io = do
    writeChan chan' (ResultTxn <$> io)
    ResultTxn txn <- takeMVar resultMVar'
    return txn

runEmpty :: Channel -> IO () -> IO ()
runEmpty (Channel { chan = chan', resultMVar = resultMVar' }) io = do
    writeChan chan' (ResultEmpty <$> io)
    _ <- takeMVar resultMVar'
    return ()

endChannel :: Channel -> IO ()
endChannel (Channel { finishMVar = finishMVar' }) =
    putMVar finishMVar' ()

--------------------------------------------------------------------------------
