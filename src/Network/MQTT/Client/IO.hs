{-# LANGUAGE LambdaCase   #-}
{-# LANGUAGE TypeFamilies #-}
module Network.MQTT.Client.IO where

import           Control.Concurrent
import           Control.Exception

import qualified Data.ByteString               as BS
import qualified Data.ByteString.Builder.Extra as BS
import qualified Data.ByteString.Unsafe        as BSU
import           Data.Word

import           Foreign.Marshal.Alloc
import           Foreign.Ptr

import           Network.MQTT.Message
import qualified Network.Transceiver           as NT

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
