{-# LANGUAGE GADTs        #-}
{-# LANGUAGE LambdaCase   #-}
{-# LANGUAGE TypeFamilies #-}
module Network.MQTT.Client.Internal where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad

import qualified Data.Binary.Get              as BG
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Builder      as BS
import qualified Data.ByteString.Builder.Extra      as BS
import qualified Data.ByteString.Lazy         as BL
import qualified Data.IntMap                  as IM
import qualified Data.Text                    as T
import qualified Data.Text.Encoding           as T
import           Network.URI
import           System.Random

import           Control.Concurrent.Broadcast
import           Network.MQTT.Client.IO
import           Network.MQTT.Client.Types
import           Network.MQTT.Message
import qualified Network.Transceiver          as NT

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

maintainConnection ::
     (NT.Data a ~ BS.ByteString, NT.Connectable a, NT.StreamConnection a)
  => Client t1
  -> a
  -> t2
  -> IO ()
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

handleInput client connection inp
  | BS.null inp = handleInput' client connection =<< NT.receiveChunk connection
  | otherwise = handleInput' client connection inp

handleInput' client connection inp = do
  decode decoder inp
  where
    decoder :: BG.Decoder ServerPacket
    decoder = BG.runGetIncremental serverPacketParser
    decode (BG.Done _leftover consumed clientPacket) inp =
      f clientPacket >>
      handleInput' client connection (BS.drop (fromIntegral consumed) inp)
    decode (BG.Fail _leftover _consumed err) _ =
      throwIO $ ClientExceptionProtocolViolation err
    decode (BG.Partial cont) inp = do
      continued <- NT.receiveChunk connection
      if BS.null continued
        then throwIO ClientExceptionServerLostSession
        else decode (cont (Just continued)) inp
    -- to be written.
    f :: ServerPacket -> IO ()
    f (ServerConnectionAccepted (SessionPresent serverSession)) = pure ()
    takehead =
      \case
        (BS.Chunk bs _) -> Just bs
        _ -> Nothing
    drophead =
      \case
        (BS.Chunk _ bs) -> Just bs
        _ -> Nothing

connectTransmitter client connection =
  NT.connect connection =<< readMVar undefined

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
