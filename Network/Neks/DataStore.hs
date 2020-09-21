module Network.Neks.DataStore (
        DataStore, createStore, createStoreIO, dump, load, loadIO, insert, insertIfNew, combine, get, delete, catDataStore
) where

import qualified Data.Map.Strict as Map (Map, empty, insert, lookup, delete, toList)
import Control.Concurrent.STM (STM, atomically)
import Control.Concurrent.STM.TMVar (TMVar, newTMVar, newTMVarIO, takeTMVar, putTMVar, readTMVar)
import Data.Hashable (Hashable, hash)
import Data.Vector as Vector (Vector, fromList, toList, mapM, (!))
import Data.Bits ((.&.))
import Data.List (sortBy)
import Data.Function (on)
import Data.Time.LocalTime (getZonedTime)

newtype DataStore k v = DataStore {mapsOf :: Vector (TMVar (Map.Map k v))}

createStore :: STM (DataStore k v)
createStore = load [Map.empty | _ <- [0..4096 :: Int]]

createStoreIO :: IO (DataStore k v)
createStoreIO = loadIO [Map.empty | _ <- [0..4096 :: Int]]

insert :: (Hashable k, Ord k) => k -> v -> DataStore k v -> STM ()
insert k v (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map0 <- takeTMVar atomicMap
        putTMVar atomicMap $! Map.insert k v map0

insertIfNew :: (Hashable k, Ord k) => k -> v -> DataStore k v -> STM (Maybe v)
insertIfNew k v (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map0 <- takeTMVar atomicMap
        case Map.lookup k map0 of
          Nothing -> do
            putTMVar atomicMap $! Map.insert k v map0
            return Nothing
          r@(Just _v0) -> do
            putTMVar atomicMap map0
            return r

combine :: (Hashable k, Ord k) => (v -> v -> v) -> k -> v -> DataStore k v -> STM ()
combine f k v (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map0 <- takeTMVar atomicMap
        case Map.lookup k map0 of
          Nothing -> do
            putTMVar atomicMap $! Map.insert k v map0
            return ()
          Just v0 -> do
            putTMVar atomicMap $! Map.insert k (f v0 v) map0
            return ()

get :: (Hashable k, Ord k) => k -> DataStore k v -> STM (Maybe v)
get k (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map0 <- readTMVar atomicMap
        return (Map.lookup k map0)

delete :: (Hashable k, Ord k) => k -> DataStore k v -> STM ()
delete k (DataStore maps) = do
        let atomicMap = maps ! (hash k .&. 0xFFF)
        map0 <- takeTMVar atomicMap
        putTMVar atomicMap $! Map.delete k map0

dump :: DataStore k v -> STM [Map.Map k v]
dump = Prelude.mapM readTMVar . toList . mapsOf

load :: [Map.Map k v] -> STM (DataStore k v)
load = fmap DataStore . Vector.mapM newTMVar . fromList

loadIO :: [Map.Map k v] -> IO (DataStore k v)
loadIO = fmap DataStore . Vector.mapM newTMVarIO . fromList

catDataStore :: (Show k, Show v, Ord k) => DataStore k v -> IO ()
catDataStore store = do
        maps <- atomically $ dump store
        zt <- getZonedTime
        let  kvs = sortBy (compare `on` fst) $ Map.toList =<< maps
             showKV (k, v) = shows k $ "\t\t" ++ show v
        mapM_ putStrLn  . ("" :) . ((:) $ "Neks.DataStore at " ++ show zt)
                        $ map showKV kvs
