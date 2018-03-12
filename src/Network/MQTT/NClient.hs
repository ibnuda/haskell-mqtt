{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE TypeFamilies      #-}
module Network.MQTT.NClient where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import qualified Data.IntMap                  as IM
import qualified Data.Text                    as T
import qualified Data.Text.Encoding           as T
import           Data.Typeable
import           Data.Word
import           Network.URI
import           System.Random

import           Control.Concurrent.Broadcast
import           Network.MQTT.Message
import qualified Network.Transceiver          as NT

data ClientConfiguration t = ClientConfiguration
  { clientConfigurationURI                       :: URI
  , clientConfigurationWill                      :: Maybe Message
  , clientConfigurationKeepAlive                 :: KeepAliveInterval
  , clientConfigurationIdentifierPrefix          :: String
  , clientConfigurationMaxUnacknowledgedMessages :: Word16
  , clientConfigurationNewTransceiver            :: IO t
  }

data ClientConfigurationException =
  ClientConfigurationException String
  deriving (Show, Typeable)

instance Exception ClientConfigurationException

validateClientConfiguration :: ClientConfiguration t -> IO ()
validateClientConfiguration clientConfiguration = do
  validateURIScheme
  validateUriAuthority
  where
    validateURIScheme =
      case uriScheme (clientConfigurationURI clientConfiguration) of
        "mqtt"  -> pure ()
        "mqtts" -> pure ()
        "ws"    -> pure ()
        "wss"   -> pure ()
        _       -> err "clientConfigurationURI: unsupported scheme."
    validateUriAuthority =
      case uriAuthority (clientConfigurationURI clientConfiguration) of
        Nothing -> err "clientConfigurationURI: missing authority."
        Just _  -> pure ()
    err = throwIO . ClientConfigurationException

uriUsername :: URI -> Maybe Username
uriUsername uri =
  f $ takeWhile (/= ':') . uriUserInfo <$> uriAuthority uri
  where
    f (Just []) = Nothing
    f (Just xs) = Just (Username . T.pack $ xs)
    f _         = Nothing

uriPassword :: URI -> Maybe Password
uriPassword uri =
  f $ drop 1 . takeWhile (/= ':') . uriUserInfo <$> uriAuthority uri
  where
    f (Just []) = Nothing
    f (Just xs) = Just (Password . T.encodeUtf8 . T.pack $ xs)
    f _         = Nothing

data ClientEvent
  = ClientEventStarted
  | ClientEventConnecting
  | ClientEventConnected
  | ClientEventPing
  | ClientEventPong
  | ClientEventReceive Message
  | ClientEventDisconnected ClientException
  | ClientEventStopped
  deriving (Eq, Ord, Show)

data ClientException
  = ClientExceptionProtocolViolation String
  | ClientExceptionConnectionRefused RejectReason
  | ClientExceptionClientLostSession
  | ClientExceptionServerLostConnection
  | ClientExceptionClientClosedConnection
  | ClientExceptionServerClosedConnection
  deriving (Eq, Ord, Show, Typeable)

-- | Anything that is not released by client yet.
newtype InboundState =
  InboundStateNotReleasedPublish Message

-- | Anything that has been released by client.
data OutboundState
  = OutboundStateNotAcknowledgedPublish ClientPacket (MVar ())
  | OutboundStateNotReceivedPublish ClientPacket (MVar ())
  | OutboundStateNotCompletedPublish (MVar ())
  | OutboundStateNotAcknowledgedSubscribe ClientPacket (MVar [Maybe QoS])
  | OutboundStateNotAcknowledgedUnsubscribe ClientPacket (MVar ())

-- | Client's "state".
data Client t = Client
  { clientIdentifier     :: ClientIdentifier
  , clientEventBroadcast :: Broadcast ClientEvent
  , clientConfiguration  :: MVar (ClientConfiguration t)
  , clientRecentActivity :: MVar Bool
  , clientOutput         :: MVar (Either ClientPacket (PacketIdentifier -> (ClientPacket, OutboundState)))
  , clientInboundState   :: MVar (IM.IntMap InboundState)
  , clientOutboundState  :: MVar ([Int], IM.IntMap OutboundState)
  , clientThreads        :: MVar (Async ())
  }

newClient :: ClientConfiguration t -> IO (Client t)
newClient clientConf =
  Client
  <$> (ClientIdentifier . T.pack . take clientIdentifierLength .
       (clientConfigurationIdentifierPrefix clientConf ++) .
       randomRs clientIdentifierCharacterRange <$> newStdGen)
  <*> newBroadcast
  <*> newMVar clientConf
  <*> newMVar False
  <*> newEmptyMVar
  <*> newMVar IM.empty
  <*> newMVar ([0 .. fromIntegral (clientConfigurationMaxUnacknowledgedMessages clientConf)], IM.empty)
  <*> (newMVar =<< async (pure ()))
  where
    clientIdentifierLength = 23
    clientIdentifierCharacterRange = ('a', 'z')

stop :: NT.Closable s => Client s -> IO ()
stop conf = do
  thrd <- readMVar (clientThreads conf)
  putMVar (clientOutput conf) (Left ClientDisconnect)
  wait thrd

subscribe :: Client s -> [(Filter, QoS)] -> IO [Maybe QoS]
subscribe _ [] = pure []
subscribe client topics = do
  response <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f response
  takeMVar response
  where
    f response i =
      let message = ClientSubscribe i topics
      in (message, OutboundStateNotAcknowledgedSubscribe message response)

unsubscribe :: Client s -> [Filter] -> IO ()
unsubscribe _ [] = pure ()
unsubscribe client topics = do
  confirmation <- newEmptyMVar
  putMVar (clientOutput client) $ Right $ f confirmation
  takeMVar confirmation
  where
    f confirmation i =
      let message = ClientUnsubscribe i topics
      in (message, OutboundStateNotAcknowledgedUnsubscribe message confirmation)

listenEvents :: Client s -> IO (BroadcastListener ClientEvent)
listenEvents client = listen (clientEventBroadcast client)

acceptEvents :: BroadcastListener ClientEvent -> IO ClientEvent
acceptEvents = accept
