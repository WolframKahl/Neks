module Network.Neks.Actions (
        Request(Set, SetIfNew, Get, Delete, Atomic), Reply(Found, NotFound)
) where

import Data.ByteString (ByteString)

data Request
  = Set ByteString ByteString
  | SetIfNew ByteString ByteString -- returns |Reply| as |Get|
  | Get ByteString
  | Delete ByteString
  | Atomic [Request]
  deriving (Show, Eq)
data Reply = Found ByteString | NotFound deriving (Show, Eq)
