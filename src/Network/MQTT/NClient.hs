{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE TypeFamilies      #-}
module Network.MQTT.NClient where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import qualified Data.Text                    as T
import qualified Data.Text.Encoding           as T
import           Data.Typeable
import           Data.Word
import           Network.URI

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
