using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        private readonly List<DatabaseMappingObject> _databaseMappingList;

        private const int DELTA_LOAD_POLL_INTERVAL = 10000;

        public DeltaFileConsumer(IConfigurationRoot config, ILogger<DeltaFileConsumer> logger,
            IDatabaseService dbService, ILocalCacheService localCacheService)
        {
            _config = config;
            _logger = logger;
            _dbService = dbService;
            _localCacheService = localCacheService;
            _databaseMappingList = _config.GetSection("DatabaseMapping").Get<DatabaseMappingObject[]>().ToList();
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
                        _logger.LogCritical("Full Load file has been found");
                        //throw new Exception("Full Load file has been found");
                        fullLoadFileFound = true;
                    }
                    else
                    {
                        var targetDatabaseName = _databaseMappingList
                            .Where(y => y.SourceDatabaseKey.Equals(fileObject.DatabaseName))
                            .Select(x => x.TargetDatabaseKey).First();

                        await _dbService.BulkLoadAndUpsertFile(targetDatabaseName, fileObject.TableName, fileObject.FileKey);

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