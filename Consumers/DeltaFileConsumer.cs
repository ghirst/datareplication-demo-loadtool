using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Gentrack.Tools.DataReplicationLoadTool.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Gentrack.Tools.DataReplicationLoadTool.Consumers
{
    public class DeltaFileConsumer : IDeltaFileConsumer
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private readonly IDatabaseService _dbService;
        private readonly ILocalCacheService _localCacheService;

        private const int DELTA_LOAD_POLL_INTERVAL = 10000;

        public DeltaFileConsumer(IConfigurationRoot config, ILogger<DeltaFileConsumer> logger,
            IDatabaseService dbService, ILocalCacheService localCacheService)
        {
            _config = config;
            _logger = logger;
            _dbService = dbService;
            _localCacheService = localCacheService;
        }

        public async Task StartPolling(ConcurrentQueue<FileObject> fileQueue, CancellationToken cancelToken)
        {
            _logger.LogInformation("Starting Delta File Consumer");

            var fullLoadFileFound = false;

            // use Semaphore to control the number of threads
            while (!cancelToken.IsCancellationRequested && !fullLoadFileFound)
            {

                if (fileQueue.Count > 0 && fileQueue.TryDequeue(out var fileObject))
                {
                    _logger.LogInformation($"Processing file {fileObject.FileKey}");

                    if (fileObject.FileName.StartsWith("LOAD"))
                    {
                        //assume we found an incremental file so its time to shut down the full load
                        _logger.LogDebug("Full Load file has been found ");
                        fullLoadFileFound = true;
                    }
                    else
                    {
                        await _dbService.BulkLoadAndUpsertFile(fileObject.DatabaseName, fileObject.TableName, fileObject.FileKey);

                        _localCacheService.MarkFileAsDone(fileObject.FileKey);
                    }
                }
                else
                {
                    _logger.LogInformation("Delta File Consumer:: Queue Empty ");
                    await Task.Delay(DELTA_LOAD_POLL_INTERVAL, cancelToken);
                }
            }

            if (fullLoadFileFound)
            {
                _logger.LogInformation("Full Load file has been found - shutting down Delta Load Process");
            }
        }
    }
}