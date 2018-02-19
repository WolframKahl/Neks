{-# LANGUAGE OverloadedStrings #-}

module Network.Neks.Client where

import Network.Neks.Message (formatRequests, parseResponses)
import Network.Neks.NetPack (netWrite, netRead)
import Network.Neks.Actions ( Request(Set, SetIfNew, Get, Delete, Atomic)
                            , Reply(Found, NotFound) )

import Network (HostName, PortID)
import qualified Network as Net (connectTo)
import System.IO (Handle, hClose)
-- import Data.ByteString.Char8 (pack)
import Data.ByteString (ByteString)

-- let portID = Net.PortNumber . fromInteger . read $ port

type SendRequests = [Request] -> IO (Either String [Reply])

genSet :: SendRequests -> ByteString -> ByteString -> IO (Either String ())
genSet sendRequests k v = do
        response <- sendRequests [Set k v]
        return $ case response of
                Left error -> Left $ "set: Error: " ++ error
                Right [] -> Right ()
                Right rs -> Left $ "set: Unexpected response: " ++ show rs

genSetIfNew :: SendRequests -> ByteString -> ByteString -> IO (Either String (Maybe ByteString))
genSetIfNew sendRequests k v = do
        response <- sendRequests [SetIfNew k v]
        return $ case response of
                Left error -> Left $ "setIfNew: Error: " ++ error
                Right [r] -> Right $ case r of
                  Found bs -> Just bs
                  NotFound -> Nothing
                Right rs -> Left $ "setIfNew: Unexpected response: " ++ show rs

genGet :: SendRequests -> ByteString -> IO (Either String (Maybe ByteString))
genGet sendRequests k = do
        response <- sendRequests [Get k]
        return $ case response of
                Left error -> Left $ "get: Error: " ++ error
                Right [r] -> Right $ case r of
                  Found bs -> Just bs
                  NotFound -> Nothing
                Right rs -> Left $ "get: Unexpected response: " ++ show rs

genDel :: SendRequests -> ByteString -> IO (Either String ())
genDel sendRequests k = do
        response <- sendRequests [Delete k]
        return $ case response of
                Left error -> Left $ "del: Error: " ++ error
                Right [] -> Right ()
                Right rs -> Left $ "del: Unexpected response: " ++ show rs

-- The following interface is only a slight abstraction of NeksClient.hs:

request :: Handle -> SendRequests
request server requests = do
        netWrite server $ formatRequests requests
        responseData <- netRead server
        return (responseData >>= parseResponses)

set' :: ByteString -> ByteString -> Handle -> IO (Either String ())
set' k v server = genSet (request server) k v

setIfNew' :: ByteString -> ByteString -> Handle -> IO (Either String (Maybe ByteString))
setIfNew' k v server = genSetIfNew (request server) k v

get' :: ByteString -> Handle -> IO (Either String (Maybe ByteString))
get' k server = genGet (request server) k

del' :: ByteString -> Handle -> IO (Either String ())
del' k server = genDel (request server) k

-- The following interface uses single-request connections:

sendRequestsHP :: HostName -> PortID -> SendRequests
sendRequestsHP host portID requests = do
        handle <- Net.connectTo host portID
        e <- request handle requests
        hClose handle
        return e

set :: HostName -> PortID -> ByteString -> ByteString -> IO (Either String ())
set host portID = genSet (sendRequestsHP host portID)

setIfNew :: HostName -> PortID -> ByteString -> ByteString -> IO (Either String (Maybe ByteString))
setIfNew host portID = genSetIfNew (sendRequestsHP host portID)

get :: HostName -> PortID -> ByteString -> IO (Either String (Maybe ByteString))
get host portID = genGet (sendRequestsHP host portID)

del :: HostName -> PortID -> ByteString -> IO (Either String ())
del host portID = genDel (sendRequestsHP host portID)
