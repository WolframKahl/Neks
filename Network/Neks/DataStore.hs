module Network.Neks.DataStore (
        DataStore, createStore, dump, load, insert, insertIfNew, get, delete, catDataStore
) where

import qualified Data.Map.Strict as Map (Map, empty, insert, lookup, delete, toList)
import Control.Concurrent.STM (STM, atomically)
import Control.Concurrent.STM.TMVar (TMVar, newTMVar, takeTMVar, putTMVar, readTMVar)
import Data.Hashable (Hashable, hash)
import Data.Vector as Vector (Vector, fromList, toList, mapM, (!))
import Data.Bits ((.&.))
import Data.List (sortBy)
import Data.Function (on)

newtype DataStore k v = DataStore {mapsOf :: Vector (TMVar (Map.Map k v))}

createStore :: STM (DataStore k v)
createStore = load [Map.empty | _ <- [0..4096]]

insert :: (Hashable k, Ord k) => k -> v -> DataStore k v -> STM ()
insert k v (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map <- takeTMVar atomicMap
        putTMVar atomicMap $! Map.insert k v map

insertIfNew :: (Hashable k, Ord k) => k -> v -> DataStore k v -> STM (Maybe v)
insertIfNew k v (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map <- takeTMVar atomicMap
        case Map.lookup k map of
          Nothing -> do
            putTMVar atomicMap $! Map.insert k v map
            return Nothing
          r@(Just _v0) -> do
            putTMVar atomicMap map
            return r

get :: (Hashable k, Ord k) => k -> DataStore k v -> STM (Maybe v)
get k (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map <- readTMVar atomicMap
        return (Map.lookup k map)

delete :: (Hashable k, Ord k) => k -> DataStore k v -> STM ()
delete k (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map <- takeTMVar atomicMap
        putTMVar atomicMap $! Map.delete k map

dump :: DataStore k v -> STM [Map.Map k v]
dump = Prelude.mapM readTMVar . toList . mapsOf

load :: [Map.Map k v] -> STM (DataStore k v)
load = fmap DataStore . Vector.mapM newTMVar . fromList

catDataStore :: (Show k, Show v, Ord k) => DataStore k v -> IO ()
catDataStore store = do
        maps <- atomically $ dump store
        let  kvs = sortBy (compare `on` fst) $ Map.toList =<< maps
             showKV (k, v) = shows k $ "\t\t" ++ show v
        mapM_ (putStrLn . showKV) kvs
