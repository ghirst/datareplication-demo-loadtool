using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Gentrack.Tools.DataReplicationLoadTool.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Binder;
using Microsoft.Extensions.Logging;

namespace Gentrack.Tools.DataReplicationLoadTool.Consumers
{
    class FullLoadFileConsumer : IFullLoadFileConsumer
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private readonly IDatabaseService _dbService;
        private readonly ILocalCacheService _localCacheService;
        private readonly int _parallelFullLoadStreams;
        private readonly List<DatabaseMappingObject> _databaseMappingList;
        private readonly string _sourceDatabaseKey;

        private const int FULL_LOAD_POLL_INTERVAL = 10000;

        public FullLoadFileConsumer(IConfigurationRoot config, ILogger<FullLoadFileConsumer> logger,
            IDatabaseService dbService, ILocalCacheService localCacheService)
        {
            _config = config;
            _logger = logger;
            _parallelFullLoadStreams = _config.GetValue<int>("ParallelFullLoadStreams");
            _dbService = dbService;
            _localCacheService = localCacheService;
            _databaseMappingList = _config.GetSection("DatabaseMapping").Get<DatabaseMappingObject[]>().ToList();
        }

        public async Task StartPolling(ConcurrentQueue<FileObject> fileQueue, CancellationToken cancelToken)
        {
            _logger.LogInformation("Starting FullLoad File Consumer");

            var incrementalFileFound = false;

            // use Semaphore to control the number of threads
            using (var concurrencySemaphore = new SemaphoreSlim(_parallelFullLoadStreams))
            {
                var tasks = new List<Task>();

                while (!cancelToken.IsCancellationRequested && !incrementalFileFound )
                {
                    if (fileQueue.Count > 0 && fileQueue.TryDequeue(out var fileObject))
                    {
                        _logger.LogInformation($"Processing file {fileObject.FileKey}");

                        concurrencySemaphore.Wait(cancelToken);
                        var thisTask = Task.Run(async () =>
                        {
                            try
                            {
                                if (!fileObject.FileName.StartsWith("LOAD"))
                                {
                                    //assume we found an incremental file so its time to shut down the full load
                                    _logger.LogDebug("Delta file has been found ");
                                    incrementalFileFound = true;
                                }
                                else
                                {
                                    var targetDatabaseName = _databaseMappingList
                                        .Where(y => y.SourceDatabaseKey.Equals(fileObject.DatabaseName))
                                        .Select(x => x.TargetDatabaseKey).First();

                                    await _dbService.BulkLoadFile(targetDatabaseName, fileObject.TableName,
                                        fileObject.FileKey);

                                    _localCacheService.MarkFileAsDone(fileObject.FileKey);
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.LogCritical(e.ToString());
                                throw e;
                            }
                            finally
                            {
                                concurrencySemaphore.Release();
                            }
                        });

                        tasks.Add(thisTask);
                    }
                    else
                    {
                        _logger.LogInformation("FullLoad File Consumer:: Queue Empty ");
                        await Task.Delay(FULL_LOAD_POLL_INTERVAL, cancelToken);
                    }
                }

                await Task.WhenAll(tasks.ToArray());

                if (incrementalFileFound)
                {
                    _logger.LogInformation("Delta file has been found - shutting down Full Load Process");
                }
            }
        }
    }
}