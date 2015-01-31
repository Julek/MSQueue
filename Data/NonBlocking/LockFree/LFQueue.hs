{-# LANGUAGE BangPatterns, MagicHash #-}
module Data.NonBlocking.LockFree.LFQueue(LFQueue(), newLFQueue, dequeueLFQueue, enqueueLFQueue) where

import Control.Concurrent.STM (STM())
import Control.Concurrent.STM.TVar (TVar())
import Control.Monad (when)
import Control.Monad.Loops (whileM_)
import Control.Monad.Ref (MonadAtomicRef, newRef, readRef, writeRef, atomicModifyRef)
import Data.IORef (IORef())
import Data.Maybe(isNothing, fromJust)
import GHC.Exts (Int(I#))
import GHC.Prim (reallyUnsafePtrEquality#)

type LFQueueIO a = LFQueue IORef a
type LFQueueSTM a = LFQueue TVar a

data LFQueue r a = LFQueue (r (r (Maybe (LFQueueElem r a)))) (r (r (Maybe (LFQueueElem r a))))
data LFQueueElem r a = LFQueueElem a (r (Maybe (LFQueueElem r a)))

instance Eq a => Eq (LFQueueElem r a) where
  (LFQueueElem x r1) == (LFQueueElem y r2) = (x == y) && ptrEq r1 r2

{-# SPECIALIZE newLFQueue :: IO (LFQueueIO a) #-}
{-# SPECIALIZE newLFQueue :: STM (LFQueueSTM a) #-}
newLFQueue :: (MonadAtomicRef r m) => m (LFQueue r a)
newLFQueue = do
  null <- newRef Nothing
  topRefRef <- newRef null
  lastRefRef <- newRef null
  return (LFQueue topRefRef lastRefRef)

{-# SPECIALIZE dequeueLFQueue :: LFQueueIO a -> IO (Maybe a) #-}
{-# SPECIALIZE dequeueLFQueue :: LFQueueSTM a -> STM (Maybe a) #-}
dequeueLFQueue :: (MonadAtomicRef r m) => LFQueue r a -> m (Maybe a)
dequeueLFQueue (LFQueue topRefRef _) = do
  res <- newRef Nothing
  whileM_ (readRef res >>= return . isNothing) $ do
    topRef <- readRef topRefRef
    top    <- readRef topRef
    case top of
      (Just (LFQueueElem v tailRef)) -> do
        b <- casRef topRefRef topRef tailRef
        when b $ writeRef res (Just (Just v))
      Nothing -> writeRef res (Just Nothing)
  readRef res >>= return . fromJust

{-# SPECIALIZE enqueueLFQueue :: LFQueueIO a -> a -> IO () #-}
{-# SPECIALIZE enqueueLFQueue :: LFQueueSTM a -> a -> STM () #-}
enqueueLFQueue :: (MonadAtomicRef r m) => LFQueue r a -> a -> m ()
enqueueLFQueue (LFQueue _ lastRefRef) v = do
  nLastRef <- newRef Nothing
  let nLastElem = LFQueueElem v nLastRef
  done <- newRef False
  whileM_ (readRef done >>= return . not) $ do
    lastRef <- readRef lastRefRef
    last <- readRef lastRef
    if (isNothing last)
      then do
        b <- atomicModifyRef lastRef (\val -> let b = isNothing val in (if b then (Just nLastElem) else val, b))
        when b $ do
          casRef lastRefRef lastRef nLastRef
          writeRef done True
      else do
        let (Just (LFQueueElem _ nextRef)) = last
        casRef lastRefRef lastRef nextRef
        return ()

{-# SPECIALIZE casRef :: IORef (IORef a) -> IORef a -> IORef a -> IO Bool #-}
{-# SPECIALIZE casRef :: TVar (TVar a) -> TVar a -> TVar a -> STM Bool #-}
casRef :: (MonadAtomicRef r m) => r (r a) -> r a -> r a -> m Bool
casRef ref comp rep = atomicModifyRef ref (\val -> let b = ptrEq val comp in (if b then rep else val, b))

{-# NOINLINE ptrEq #-}
ptrEq :: a -> a -> Bool
ptrEq !x !y = I# (reallyUnsafePtrEquality# x y) == 1
