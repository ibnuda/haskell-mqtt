module Network.MQTT.Client.Types where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import qualified Data.IntMap                  as IM
import           Data.Typeable
import           Data.Word
import           Network.URI

import           Control.Concurrent.Broadcast
import           Network.MQTT.Message

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

data ClientEvent
  = ClientEventStarted
  | ClientEventConnecting
  | ClientEventConnected
  | ClientEventPing
  | ClientEventPong
  | ClientEventReceived Message
  | ClientEventDisconnected ClientException
  | ClientEventStopped
  deriving (Eq, Ord, Show)

data ClientException
  = ClientExceptionProtocolViolation String
  | ClientExceptionConnectionRefused RejectReason
  | ClientExceptionClientLostSession
  | ClientExceptionServerLostSession
  | ClientExceptionClientClosedConnection
  | ClientExceptionServerClosedConnection
  deriving (Eq, Ord, Show, Typeable)

instance Exception ClientException

-- | Anything that is not released by client yet.
newtype InboundState =
  InboundStateNotReleasedPublish Message

-- | Anything that has been released by client.
data OutboundState
  = OutboundStateNotAcknowledgedPublish ClientPacket (MVar ())
  | OutboundStateNotReceivedPublish ClientPacket (MVar ())
  | OutboundStateNotCompletePublish (MVar ())
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
