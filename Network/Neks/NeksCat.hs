{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import System.IO (hPutStrLn, stderr)
import System.Environment (getArgs)
import Data.ByteString (ByteString)

import Network.Neks.Disk (loadFrom)
import Network.Neks.DataStore (DataStore, catDataStore)

type Store = DataStore ByteString ByteString

main :: IO ()
main = do
        args <- getArgs
        case args of
                "--help" : _ -> hPutStrLn stderr instructions -- To be explicit
                [storeFile] -> catDataStoreFile storeFile
                [] -> catDataStoreFile "store.kvs"
                _          -> hPutStrLn stderr instructions

catDataStoreFile :: FilePath -> IO ()
catDataStoreFile storeFile = do
        loaded <- loadFrom storeFile
        case loaded of
                Nothing -> hPutStrLn stderr $ "Could not load from " ++ storeFile
                Just store -> catDataStore (store :: Store)

instructions :: String
instructions = "Usage: NeksCat <opt-args>\n" ++
               "<opt-args> can be empty or a store file name"
