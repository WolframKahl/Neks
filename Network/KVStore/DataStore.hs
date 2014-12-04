module Network.KVStore.DataStore (
	BinaryStore,
	createStore,
	insert,
	get
) where

import qualified Data.Map.Strict as Map (Map, empty, insert, lookup)
import Control.Concurrent.STM (STM)
import Control.Concurrent.STM.TMVar (TMVar, newTMVar, takeTMVar, putTMVar, readTMVar)
import Data.ByteString as BS (ByteString, foldl)
import Data.Vector (Vector, fromList, (!))

newtype DataStore k v = DataStore {mapsOf :: Vector (TMVar (Map.Map k v))}

type BinaryStore = DataStore ByteString ByteString

createStore :: Ord k => STM (DataStore k v)
createStore = do
	maps <- sequence [newTMVar Map.empty | _ <- [0..255]]
	return (DataStore (fromList maps))

insert :: ByteString -> ByteString -> BinaryStore -> STM ()
insert k v (DataStore maps) = do
	let atomicMap = maps ! hash k
	map <- takeTMVar atomicMap
	putTMVar atomicMap $! (Map.insert k v map)

get :: ByteString -> BinaryStore -> STM (Maybe ByteString)
get k (DataStore maps) = do
	let atomicMap = maps ! hash k
	map <- readTMVar atomicMap
	return (Map.lookup k map)

-- Produces a number in the range 0-255
hash :: Num b => ByteString -> b
hash = fromIntegral . (BS.foldl (+) 0)