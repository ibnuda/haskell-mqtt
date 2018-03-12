module Control.Concurrent.Broadcast where

import           Control.Concurrent.MVar
import           Control.Exception
import           Data.Typeable

-- | Something akin to linked-list datatype for the concurrent values.
data Tail a =
  Tail (MVar (Maybe (Tail a, a)))

newtype Broadcast a =
  Broadcast (MVar (Tail a))

newtype BroadcastListener a =
  BroadcastListener (MVar (Tail a))

data BroadcastTerminatedException =
  BroadcastTerminatedException
  deriving (Show, Typeable)

instance Exception BroadcastTerminatedException

newBroadcast :: IO (Broadcast a)
newBroadcast = Broadcast <$> (newMVar =<< Tail <$> newEmptyMVar)

broadcast :: Broadcast a -> a -> IO ()
broadcast (Broadcast mt) a =
  modifyMVar_ mt $ \(Tail currentTail) -> do
    newTail <- Tail <$> newEmptyMVar
    putMVar currentTail (Just (newTail, a))
    pure newTail

terminate :: Broadcast a -> IO ()
terminate (Broadcast mt) =
  modifyMVar_ mt $ \(Tail currentTail) -> do
    putMVar currentTail Nothing
    pure (Tail currentTail)

listen :: Broadcast a -> IO (BroadcastListener a)
listen (Broadcast mt) = BroadcastListener <$> (newMVar =<< readMVar mt)

tryAccept :: BroadcastListener a -> IO (Maybe a)
tryAccept (BroadcastListener mt) =
  modifyMVar mt $ \(Tail oldTail) -> do
    mat <- readMVar oldTail
    case mat of
      Nothing           -> pure (Tail oldTail, Nothing)
      Just (newTail, a) -> pure (newTail, Just a)

accept :: BroadcastListener a -> IO a
accept bl = do
  ma <- tryAccept bl
  case ma of
    Nothing -> throwIO BroadcastTerminatedException
    Just a  -> pure a
