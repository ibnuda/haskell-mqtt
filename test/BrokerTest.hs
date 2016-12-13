{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
module BrokerTest ( getTestTree ) where

import           Control.Exception
import           Control.Concurrent.MVar
import           Data.Typeable
import qualified Data.Sequence as Seq
import Data.Monoid
import Control.Applicative

import           Network.MQTT.Message
import           Network.MQTT.Authentication
import qualified Network.MQTT.Broker as Broker
import qualified Network.MQTT.Topic as Topic
import qualified Network.MQTT.Session as Session
import qualified Network.MQTT.Message as Message

import           Test.Tasty
import           Test.Tasty.HUnit

newtype TestAuthenticator = TestAuthenticator (AuthenticatorConfig TestAuthenticator)

instance Authenticator TestAuthenticator where
  data Principal TestAuthenticator = TestPrincipal deriving (Eq, Ord, Show)
  data AuthenticatorConfig TestAuthenticator = TestAuthenticatorConfig
    { cfgAuthenticate           :: ConnectionRequest -> IO (Maybe (Principal TestAuthenticator))
    , cfgHasPublishPermission   :: Principal TestAuthenticator -> Topic.Topic  -> IO Bool
    , cfgHasSubscribePermission :: Principal TestAuthenticator -> Topic.Filter -> IO Bool
    }
  data AuthenticationException TestAuthenticator = TestAuthenticatorException deriving (Typeable, Show)
  newAuthenticator = pure . TestAuthenticator
  authenticate (TestAuthenticator cfg) = cfgAuthenticate cfg
  hasPublishPermission (TestAuthenticator cfg) = cfgHasPublishPermission cfg
  hasSubscribePermission (TestAuthenticator cfg) = cfgHasSubscribePermission cfg

instance Exception (AuthenticationException TestAuthenticator)

getTestTree :: IO TestTree
getTestTree =
  pure $ testGroup "Broker"
    [ testGroup "Authentication"
      [ testCase "Reject with 'ServerUnavaible' when authentication throws exception" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.new $ TestAuthenticator $ authenticatorConfig { cfgAuthenticate = const $ throwIO TestAuthenticatorException }
          let sessionRejectHandler                           = putMVar m1
              sessionAcceptHandler session present principal = putMVar m2 (session, present, principal)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just ServerUnavailable @?= x
          tryReadMVar m2 >>= \x-> Nothing                @?= x
      , testCase "Reject 'NotAuthorized' when authentication returned Nothing" $ do
          m1 <- newEmptyMVar
          m2 <- newEmptyMVar
          broker <- Broker.new $ TestAuthenticator $ authenticatorConfig { cfgAuthenticate = const $ pure Nothing }
          let sessionRejectHandler                           = putMVar m1
              sessionAcceptHandler session present principal = putMVar m2 (session, present, principal)
          Broker.withSession broker connectionRequest sessionRejectHandler sessionAcceptHandler
          tryReadMVar m1 >>= \x-> Just NotAuthorized   @?= x
          tryReadMVar m2 >>= \x-> Nothing              @?= x
      ]
    , testGroup "Subscriptions"

      [ testCase "subscribe the same filter from 2 different sessions" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              req2 = connectionRequest { requestClientIdentifier = "2" }
              msg  = Message.Message "a/b" "" Qos0 False False
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ _->
            Broker.withSession broker req2 (const $ pure ()) $ \session2 _ _-> do
              Broker.subscribe broker session1 42 [("a/b", Qos0)]
              Broker.subscribe broker session2 47 [("a/b", Qos0)]
              Broker.publishDownstream broker msg
              queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
              queue2 <- (<>) <$> Session.dequeue session2 <*> Session.dequeue session2
              queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged 42 [Just Qos0], ServerPublish (-1) msg]
              queue2 @?= Seq.fromList [ ServerSubscribeAcknowledged 47 [Just Qos0], ServerPublish (-1) msg]

      , testCase "get retained message on subscription (newer overrides older, issue #6)" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              msg1 = Message.Message "topic" "test"  Qos0 True False
              msg2 = Message.Message "topic" "toast" Qos0 True False
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ _-> do
            Broker.publishDownstream broker msg1
            Broker.publishDownstream broker msg2
            Broker.subscribe broker session1 23 [("topic", Qos0)]
            queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
            queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged 23 [Just Qos0], ServerPublish (-1) msg2]

      , testCase "delete retained message when body is empty" $ do
          broker <- Broker.new $ TestAuthenticator authenticatorConfigAllAccess
          let req1 = connectionRequest { requestClientIdentifier = "1" }
              msg1 = Message.Message "topic" "test"  Qos0 True False
              msg2 = Message.Message "topic" "" Qos0 True False
          Broker.withSession broker req1 (const $ pure ()) $ \session1 _ _-> do
            Broker.publishDownstream broker msg1
            Broker.publishDownstream broker msg2
            Broker.subscribe broker session1 23 [("topic", Qos0)]
            queue1 <- (<>) <$> Session.dequeue session1 <*> Session.dequeue session1
            queue1 @?= Seq.fromList [ ServerSubscribeAcknowledged 23 [Just Qos0] ]
      ]
    ]

authenticatorConfig :: AuthenticatorConfig TestAuthenticator
authenticatorConfig  = TestAuthenticatorConfig
  { cfgAuthenticate           = const (pure Nothing)
  , cfgHasPublishPermission   = \_ _-> pure False
  , cfgHasSubscribePermission = \_ _-> pure False
  }

authenticatorConfigAllAccess :: AuthenticatorConfig TestAuthenticator
authenticatorConfigAllAccess = TestAuthenticatorConfig
  { cfgAuthenticate           = const (pure $ Just TestPrincipal)
  , cfgHasPublishPermission   = \_ _-> pure True
  , cfgHasSubscribePermission = \_ _-> pure True
  }

connectionRequest :: ConnectionRequest
connectionRequest  = ConnectionRequest
  { requestClientIdentifier = "mqtt-default"
  , requestCleanSession     = True
  , requestSecure           = False
  , requestCredentials      = Nothing
  , requestHttp             = Nothing
  , requestCertificateChain = Nothing
  , requestRemoteAddress    = Nothing
  }
