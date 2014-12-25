module Main where

import qualified Network as Net
import System.IO (Handle, hClose)
import Data.ByteString (ByteString)
import Control.Monad (forever, unless)
import Control.Concurrent (ThreadId, forkIO, threadDelay)
import Control.Concurrent.STM (STM, atomically)
import Control.Exception (SomeException, catch, finally) 
import Network.KVStore.NetPack (netRead, netWrite)
import Network.KVStore.Message (parseRequests, formatResponses)
import Network.KVStore.DataStore (DataStore, createStore, insert, get, delete)
import Network.KVStore.Actions (Request(Set, Get, Delete, Atomic), Reply(Found, NotFound))
import Network.KVStore.Disk (saveTo, loadFrom)

type Store = DataStore ByteString ByteString

main = do
	globalStore <- loadFrom "store.kvs" >>= \loaded -> case loaded of
		Just store -> return store
		Nothing -> atomically createStore
	let periodically action = forever (threadDelay (30*1000*1000) >> action)
	forkIO $ periodically (saveTo "store.kvs" globalStore)
	serve globalStore

serve :: Store -> IO ()
serve store = Net.withSocketsDo $ do
	sock <- Net.listenOn (Net.PortNumber 9999)
	forever (wait sock store)

wait :: Net.Socket -> Store -> IO ThreadId
wait sock store = do
	(client, _, _) <- Net.accept sock
	forkIO (run client)
	where
	run client = (handle client store `finally` hClose client) `catch` exceptionHandler
	exceptionHandler :: SomeException -> IO ()
	exceptionHandler exception = return () -- Optional exception reporting

handle :: Handle -> Store -> IO ()
handle client store = do
	result <- processRequests client store
	case result of
		Right success -> handle client store
		Left failure -> return () -- Optional soft error reporting

processRequests :: Handle -> Store -> IO (Either String ())
processRequests client store = do
	requestData <- netRead client
	case requestData >>= parseRequests of
		Left err -> return (Left err)
		Right requests -> do
			results <- mapM (atomically . processWith store) requests
			netWrite client . formatResponses . concat $ results
			return (Right ())

processWith :: Store -> Request -> STM [Reply]
processWith store (Set k v) = do
	insert k v store
	return []
processWith store (Get k) = do
	result <- get k store
	return $ case result of
		Nothing -> [NotFound]
		Just v -> [Found v]
processWith store (Delete k) = do
	delete k store
	return []
processWith store (Atomic requests) = do
	results <- mapM (processWith store) requests
	return (concat results)