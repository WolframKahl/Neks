module Network.Neks.NetPack (netWrite, netRead) where

import Data.ByteString as BS (ByteString, hGet, length, hPut, append)
import Data.Serialize (encode, decode)
import System.IO (Handle)
import Data.Word (Word64)

netWrite :: Handle -> ByteString -> IO ()
netWrite hdl msg = do
        let len = fromIntegral (BS.length msg) :: Word64
        BS.hPut hdl (encode len `append` msg)

netRead :: Handle -> IO (Either String ByteString)
netRead hdl = do
        lengthBytes <- hGet hdl 8
        case decode lengthBytes of
                Left _err -> return (Left "Network stream ended while reading length")
                Right len  | len < (10^(6 :: Word) :: Word64) -> readData (fromIntegral len)
                           | otherwise -> return (Left "Message too long")
        where readData len = do
                dataBytes <- hGet hdl len
                if BS.length dataBytes /= len
                        then return (Left "Connection dropped during message read")
                        else return (Right dataBytes)
