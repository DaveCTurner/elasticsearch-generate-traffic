{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

{- Configure index like this:

DELETE /synctest

PUT /synctest
{"settings":{"index":{"number_of_replicas":1,"number_of_shards":1}},"mappings":{"testdoc":{"properties":{"serial":{"type":"integer"},"updated":{"type":"long"}}}}}

-}

import Data.Aeson.Lens
import Control.Lens ((^..))
import System.IO
import Data.Monoid
import System.Random
import qualified Data.Text as T
import qualified Data.Sequence as S
import Data.Time
import Control.Exception
import Control.Monad
import qualified Codec.Compression.GZip as GZ
import qualified Data.ByteString.Lazy as BL
import Data.Aeson
import Network.HTTP.Client
import Network.HTTP.Types.Header
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Concurrent.MVar
import Text.Printf
import Options.Applicative

data Config = Config
  { workerCount         :: Int
  , workerIterations    :: Int
  , batchSize           :: Int
  , targetDocumentCount :: Int
  , logFileName         :: String
  } deriving (Show, Eq)

config :: ParserInfo Config
config = info (parser <**> helper) fullDesc
  where
  parser = Config
    <$> option auto (long "worker-count" <> value 10 <> showDefault
                     <> help "The number of concurrent worker threads to run")
    <*> option auto (long "worker-iterations" <> value 20 <> showDefault
                     <> help "The number of bulk requests each worker should make")
    <*> option auto (long "batch-size" <> value 500 <> showDefault
                     <> help "The number of individual actions in each bulk request")
    <*> option auto (long "target-document-count" <> value 10000 <> showDefault
                     <> help "The approximate number of distinct documents")
    <*> strOption (long "log-file" <> help "File in which to record activity")

compress :: BL.ByteString -> BL.ByteString
compress = GZ.compressWith $ GZ.defaultCompressParams { GZ.compressLevel = GZ.bestCompression }

ndJson :: [Action] -> BL.ByteString
ndJson = BL.concat . concatMap linesFromAction

linesFromAction :: Action -> [BL.ByteString]
linesFromAction (IndexNewDoc doc)
  =  jsonLine (object ["index" .= object []])
  ++ jsonLine doc
linesFromAction (UpdateDoc docId doc)
  =  jsonLine (object ["update" .= object ["_id" .= docId]])
  ++ jsonLine (object ["doc" .= doc])
linesFromAction (DeleteDoc docId)
  =  jsonLine (object ["delete" .= object ["_id" .= docId]])

jsonLine :: ToJSON a => a -> [BL.ByteString]
jsonLine v = [encode v, BL.singleton 0x0a]

data Doc = Doc { docSerial :: Int, docUpdated :: T.Text }
  deriving (Show, Eq)

instance ToJSON Doc where
  toJSON Doc{..} = object
    [ "serial"  .= docSerial
    , "updated" .= docUpdated
    ]

data Action
  = IndexNewDoc        Doc
  | UpdateDoc   T.Text Doc
  | DeleteDoc   T.Text
  deriving (Show, Eq)

main :: IO ()
main = do
  Config{..} <- execParser config
  withFile logFileName WriteMode $ \hLog -> do
    manager <- newManager defaultManagerSettings
    rawReq <- parseRequest "http://127.0.0.1:9200/synctest/testdoc/_bulk"

    docIdsVar <- newTVarIO S.empty
    rngVar    <- newTVarIO =<< getStdGen
    serialVar <- newTVarIO 0
    logLock   <- newMVar ()

    let req = rawReq
          { method = "POST"
          , requestHeaders = requestHeaders rawReq
              ++ [(hContentType, "application/json")
                 ,(hContentEncoding, "gzip")]
          }

        nextSerial = do
          serial <- (+1) <$> readTVar serialVar
          writeTVar serialVar serial
          return serial

        withRng f = do
          rng <- readTVar rngVar
          let (result, rng') = f rng
          writeTVar rngVar rng'
          return result

        newBulkAction = do
          atomically $ replicateM batchSize $ do
            docIds <- readTVar docIdsVar
            slot   <- withRng $ randomR (0, max targetDocumentCount $ S.length docIds)
            if slot < S.length docIds
              then do
                let docId = S.index docIds slot
                isDelete <- withRng random
                if isDelete
                  then do
                    writeTVar docIdsVar $ let (before, notBefore) = S.splitAt slot docIds in before <> S.drop 1 notBefore
                    return $ \_ -> DeleteDoc docId
                  else do
                    serial <- nextSerial
                    return $ \now -> UpdateDoc docId $ Doc serial now

              else do
                serial <- nextSerial
                return $ \now -> IndexNewDoc $ Doc serial now

        worker workerId = replicateM_ workerIterations $ do
          bulkActionConstructors <- newBulkAction
          now <- T.pack <$> show <$> negate <$> floor <$> (*1000) <$> diffUTCTime (UTCTime (fromGregorian 1970 01 01) 0) <$> getCurrentTime
          let bulkActions = map ($ now) bulkActionConstructors
              rawBody     = ndJson bulkActions
              thisReq = req { requestBody = RequestBodyLBS $ compress rawBody }

              logRequestAndResult logResult = withMVar logLock $ \() -> do
                hPutStrLn hLog $ printf "\n\n# Worker %d at %s:" (workerId :: Int) (T.unpack now)
                hPutStrLn hLog $ "POST /synctest/testdoc/_bulk"
                BL.hPut hLog rawBody
                hPutStr hLog $ "\n# Result:\n# "
                logResult

              onException :: HttpException -> IO ()
              onException e = logRequestAndResult $ hPutStr hLog $ "Exception: " ++ show e

          handle onException $ withResponse thisReq manager $ \response -> do
            wholeResponseBody <- BL.fromChunks <$> brConsume (responseBody response)

            case decode wholeResponseBody :: Maybe Value of
              Nothing -> logRequestAndResult $ do
                BL.hPut hLog wholeResponseBody
                hPutStrLn hLog "# BAD JSON"

              Just response -> do
                let itemResponses = response ^.. key "items" . values

                    newDocIds =
                      [ newDocId
                      | (IndexNewDoc _, itemResponse) <- zip bulkActions itemResponses
                      , newDocId <- itemResponse ^.. key "index" . key "_id" . _String
                      ]

                atomically $ modifyTVar docIdsVar $ (<> S.fromList newDocIds)
                logRequestAndResult $ do
                  BL.hPut hLog wholeResponseBody
                  hPutStrLn hLog $ printf "# generated ids: %s" (show newDocIds)
    
    asyncs <- mapM (async . worker) (take workerCount [0..])
    mapM_ wait asyncs

