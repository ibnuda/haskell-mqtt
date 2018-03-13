{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE TypeFamilies      #-}
module Network.MQTT.NClient where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import qualified Data.Binary.Get               as BG
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Builder       as BS
import qualified Data.ByteString.Builder.Extra as BS
import qualified Data.ByteString.Unsafe        as BSU
import qualified Data.IntMap                   as IM
import qualified Data.Text                     as T
import qualified Data.Text.Encoding            as T
import           Data.Typeable
import           Data.Word
import           Network.URI
import           System.Random

import           Foreign.Marshal.Alloc
import           Foreign.Ptr

import           Control.Concurrent.Broadcast
import           Network.MQTT.Message
import qualified Network.Transceiver           as NT

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
validateClientConfiguration clientConf = do
  validateURIScheme
  validateUriAuthority
  where
    validateURIScheme =
      case uriScheme (clientConfigurationURI clientConf) of
        "mqtt"  -> pure ()
        "mqtts" -> pure ()
        "ws"    -> pure ()
        "wss"   -> pure ()
        _       -> err "clientConfigurationURI: unsupported scheme."
    validateUriAuthority =
      case uriAuthority (clientConfigurationURI clientConf) of
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

bufferedOutput ::
     (NT.StreamConnection s, NT.Data s ~ BS.ByteString)
  => s
  -> IO ClientPacket
  -> IO (Maybe ClientPacket)
  -> (BS.ByteString -> IO ())
  -> IO ()
bufferedOutput transmitter getM getMM sendBS =
  bracket (mallocBytes bufferSize) free waitForMessage
  where
    bufferSize :: Int
    bufferSize = 1024
    waitForMessage :: Ptr Word8 -> IO ()
    waitForMessage buffer = getM >>= sendMessage buffer 0
    pollForMessage :: Ptr Word8 -> Int -> IO ()
    pollForMessage buffer pos = do
      getMM >>= \case
        Nothing -> do
          yield
          getMM >>= \case
            Nothing -> flushBuffer buffer pos >> waitForMessage buffer
            Just msg -> sendMessage buffer pos msg
        Just msg -> sendMessage buffer pos msg
    sendMessage :: Ptr Word8 -> Int -> ClientPacket -> IO ()
    sendMessage buffer pos msg = do
      pos' <- runBuilder >>= \(written, nxt) -> finishWriter (pos + written) nxt
      case msg of
        ClientDisconnect -> flushBuffer buffer pos'
        _                -> pollForMessage buffer pos'
      where
        runBuilder =
          BS.runBuilder
            (clientPacketBuilder msg)
            (plusPtr buffer pos)
            (bufferSize - pos)
        finishWriter posit (BS.Done) = pure posit
        finishWriter posit (BS.Chunk _ _) = pure posit
        finishWriter posit (BS.More _ writer) = do
          flushBuffer buffer posit
          uncurry finishWriter =<< writer buffer bufferSize
        finishWriter' posit (BS.Chunk chunk writer) = do
          flushBuffer buffer posit
          sendBS chunk
          uncurry finishWriter =<< writer buffer bufferSize
    flushBuffer buffer pos =
      BSU.unsafePackCStringLen (castPtr buffer, pos) >>= \bs ->
        NT.sendChunk transmitter bs >> pure ()
{-# INLINE bufferedOutput #-}

-- | Connecting to the tranmistter.
-- TODO: What to do? Connecting to transmitter?
connectTransmitter client connection =
  NT.connect connection =<< readMVar undefined

-- | TODO: Add.
sendConnect ::
     (NT.Data a ~ BS.ByteString, NT.StreamConnection a)
  => Client t
  -> a
  -> IO ()
sendConnect client connection = do
  conf <- readMVar (clientConfiguration client)
  NT.sendChunks connection $
    BS.toLazyByteString $
    clientPacketBuilder
      ClientConnect
      { connectClientIdentifier = clientIdentifier client
      , connectCleanSession = CleanSession False
      , connectKeepAlive = clientConfigurationKeepAlive conf
      , connectWill = clientConfigurationWill conf
      , connectCredentials = undefined
      }

-- | TODO: add.
receiveConnectAcknowledgement ::
     (NT.Data a ~ BS.ByteString, NT.StreamConnection a)
  => Bool
  -> a
  -> IO BS.ByteString
receiveConnectAcknowledgement session connection = do
  bs <- NT.receiveChunk connection
  decode decoder bs
  where
    decoder :: BG.Decoder ServerPacket
    decoder = BG.runGetIncremental serverPacketParser
    decode (BG.Done _leftover consumed clientPacket) input =
      f clientPacket >> pure (BS.drop (fromIntegral consumed) input)
    decode (BG.Fail _leftover _consumed err) _ =
      throwIO $ ClientExceptionProtocolViolation err
    decode (BG.Partial _) _ =
      throwIO $
      ClientExceptionProtocolViolation "Expected CONNACK, got end of input."
    f (ServerConnectionAccepted (SessionPresent serverSession)) =
      case (serverSession, session) of
        (True, True)  -> pure ()
        (True, False) -> throwIO ClientExceptionClientLostSession
        _             -> throwIO ClientExceptionServerLostSession
    f (ServerConnectionRejected reason) =
      throwIO $
      ClientExceptionProtocolViolation $ "expected CONNACK got " ++ show reason
    f _ =
      throwIO $
      ClientExceptionProtocolViolation $ "expected CONNACK got something else."

maintainConnection :: (NT.Data a ~ BS.ByteString, NT.Connectable a, NT.StreamConnection a) => Client t1 -> a -> t2 -> IO ()
maintainConnection client connection inp =
  keepAlive client `race_` handleOutput client connection `race_`
  (handleInput client connection inp `catch` \e -> print (e :: SomeException))

keepAlive :: Client t -> IO ()
keepAlive client = do
  interval <-
    (500000 *) . fromIntegral . clientConfigurationKeepAlive <$>
    readMVar (clientConfiguration client)
  forever $ do
    threadDelay interval
    activity <- swapMVar (clientRecentActivity client) False
    unless activity $ putMVar (clientOutput client) $ Left ClientPingRequest

getMessage :: Client t -> IO ClientPacket
getMessage client = do
  message <- takeMessage client =<< takeMVar (clientOutput client)
  void $ swapMVar (clientRecentActivity client) True
  pure message

getMaybeMessage :: Client t -> IO (Maybe ClientPacket)
getMaybeMessage client =
  tryTakeMVar (clientOutput client) >>= \case
    Nothing -> pure Nothing
    Just emsg -> Just <$> takeMessage client emsg

takeMessage ::
     Client t
  -> (Either ClientPacket (PacketIdentifier -> (ClientPacket, OutboundState)))
  -> IO ClientPacket
takeMessage _ (Left msg)        = pure msg
takeMessage client (Right imsg) = assignPacketIdentifier client imsg

assignPacketIdentifier ::
     Client t
  -> (PacketIdentifier -> (ClientPacket, OutboundState))
  -> IO ClientPacket
assignPacketIdentifier client x =
  modifyMVar (clientOutboundState client) assign >>= \case
    Just m -> pure m
    Nothing -> threadDelay 100000 >> assignPacketIdentifier client x
  where
    assign im@([], _) = pure (im, Nothing)
    assign (ix:is, m) =
      let (msg, st) = x (PacketIdentifier ix)
      in pure ((is, IM.insert ix st m), Just msg)

handleOutput ::
     (NT.Data a ~ BS.ByteString, NT.StreamConnection a, NT.Connectable a)
  => Client t
  -> a
  -> IO ()
handleOutput client connection = do
  bufferedOutput
    connection
    (getMessage client)
    (getMaybeMessage client)
    (NT.sendChunk connection)

handleInput client connection inp = undefined

handleConnection ::
     (NT.Data a ~ BS.ByteString, NT.StreamConnection a, NT.Connectable a)
  => Client t
  -> Bool
  -> a
  -> IO ()
handleConnection client clientSessionPresent connection = do
  broadcast (clientEventBroadcast client) ClientEventConnecting
  connectTransmitter client connection
  broadcast (clientEventBroadcast client) ClientEventConnected
  sendConnect client connection
  receiveConnectAcknowledgement clientSessionPresent connection >>=
    maintainConnection client connection
