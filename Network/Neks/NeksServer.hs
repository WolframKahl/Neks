{-# LANGUAGE PatternGuards #-}
module Main where

import qualified Network as Net
import System.IO (Handle, hClose)
import System.Environment (getArgs)
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import qualified Data.Char as Char
import Control.Monad (forever, unless)
import Control.Concurrent.STM (STM, atomically)
import Control.Concurrent (ThreadId, forkIO, threadDelay)
import Control.Exception (SomeException, catch, finally)
import Network.Neks.Disk (saveTo, loadFrom)
import Network.Neks.NetPack (netRead, netWrite)
import Network.Neks.Message (parseRequests, formatResponses)
import Network.Neks.DataStore (DataStore, createStore, insert, insertIfNew, combine, get, delete, catDataStore)
import Network.Neks.Actions (Request(Set, SetIfNew, Append, Get, Delete, Atomic), Reply(Found, NotFound))
import System.Posix.Signals (userDefinedSignal1, installHandler, Handler(Catch))

import Control.Applicative (pure, (<*>))
import Control.Arrow (first)
import Options

type Store = DataStore ByteString ByteString

data MainOptions = MainOptions
    { optNoPersistence :: Bool
    , optPort :: Integer
    , optKVFile :: FilePath
    }

instance Options MainOptions where
    defineOptions = pure MainOptions
        <*> simpleOption "no-persistence" False
            "Disable storing keys and values on disk."
        <*> simpleOption "port" 9999
            "Server port."
        <*> simpleOption "kvs" "store.kvs"
            "File for persistent storage."

main :: IO ()
main = runCommand $ \ opts _args -> let
    port = fromInteger $ optPort opts
    kvs = optKVFile opts
  in if optNoPersistence opts
  then serveWithoutPersistence port
  else serveWithPersistence kvs port

serveWithPersistence :: FilePath -> Net.PortNumber -> IO ()
serveWithPersistence storeFile port = do
        let delaySeconds = 30
        loaded <- loadFrom storeFile
        globalStore <- case loaded of
                Just store -> return store
                Nothing -> atomically createStore
        let periodically action = forever
                (threadDelay (delaySeconds*10^6) >> action)
        forkIO $ periodically (saveTo storeFile globalStore)
        putStrLn  . ("Serving on port " ++) . shows port
                  $ "\nSaving DB to " ++ storeFile
        serve globalStore (Net.PortNumber port)

serveWithoutPersistence :: Net.PortNumber -> IO ()
serveWithoutPersistence port = do
        globalStore <- atomically createStore
        putStrLn $ "Serving on port " ++ shows port "\nNot persisting to disk"
        serve globalStore (Net.PortNumber port)

serve :: Store -> Net.PortID -> IO ()
serve store port = do
      _ <- installHandler userDefinedSignal1 (Catch $ catDataStore store) Nothing
      Net.withSocketsDo $ do
        sock <- Net.listenOn port
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
processWith store (SetIfNew k v) = do
        result <- insertIfNew k v store
        return $ case result of
                Nothing -> [NotFound]
                Just v0 -> [Found v0]
processWith store (Append k v) = do
        combine cat k v store
        return []
   where
      cat x y =  BS.append x $ BS.cons (fromIntegral $ Char.ord '\n') y
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

instructions = "Usage: NeksServer <opt-args>\n" ++
               "<opt-args> can be empty or can be \"--no-persistence\" to disable storing keys and values on disk"
