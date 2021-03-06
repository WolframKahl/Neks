module Network.Neks.Disk (saveTo, loadFrom) where

import Control.Concurrent.STM (atomically)
import Network.Neks.DataStore (DataStore, dump, load)
import Network.Neks.NetPack (netRead, netWrite) -- also works on files
import System.IO (Handle, withFile, IOMode(ReadMode, WriteMode))
import System.Directory (renameFile, doesFileExist)
import Data.Serialize (Serialize, encode, decode)
-- import Control.Applicative ((<$>))

saveTo :: (Serialize a, Ord a, Serialize b) => String -> DataStore a b -> IO ()
saveTo path store = do
        withFile (path ++ "~") WriteMode (`saveToHandle` store)
        renameFile (path ++ "~") path

saveToHandle :: (Ord k, Serialize k, Serialize v) => Handle -> DataStore k v -> IO ()
saveToHandle handle store = do
        maps <- atomically (dump store)
        sequence_ $ map (netWrite handle . encode) maps

loadFrom :: (Serialize a, Ord a, Serialize b) => String -> IO (Maybe (DataStore a b))
loadFrom path = doesFileExist path >>= \exists -> if exists
        then Just <$> withFile path ReadMode loadFromHandle
        else return Nothing

loadFromHandle :: (Ord k, Serialize k, Serialize v) => Handle -> IO (DataStore k v)
loadFromHandle handle = do
        maps <- sequence . replicate 4096 $ do
                mapData <- netRead handle
                let Right map0 = mapData >>= decode
                return map0
        atomically (load maps)
