--------------------------------------------------------------------------------

module Database.LMDB.Streaming (readLMDB, writeLMDB) where

--------------------------------------------------------------------------------

import Control.Monad.Trans.Resource (MonadResource)
import Data.ByteString (ByteString)
import Database.LMDB.Raw (MDB_dbi', MDB_env)
import qualified Database.LMDB.Resource as R (readLMDB, writeLMDB)
import Streaming.Prelude (Of, Stream, snd', yield)
import qualified Streaming.Prelude as S (foldM)

--------------------------------------------------------------------------------

-- | A read transaction is kept open for the duration of the stream.
-- Bear in mind LMDB's caveats regarding long-lived transactions.
readLMDB :: (MonadResource m) => MDB_env -> MDB_dbi' -> Stream (Of (ByteString, ByteString)) m ()
readLMDB env dbi = R.readLMDB env dbi yield

-- | A write transaction is kept open for the duration of the stream.
-- Bear in mind LMDB's caveats regarding long-lived transactions.
writeLMDB :: (MonadResource m)
          => MDB_env
          -> MDB_dbi'
          -> Bool     -- ^ If 'True', an exception will be thrown when attempting to re-insert a key.
          -> Stream (Of (ByteString, ByteString)) m r
          -> m r
writeLMDB env dbi noOverwrite stream =
    R.writeLMDB env dbi noOverwrite $ \write -> snd' <$> S.foldM (const write) (return ()) return stream

--------------------------------------------------------------------------------
