using Gentrack.Tools.DataReplicationLoadTool.localCachePath;
using Gentrack.Tools.DataReplicationLoadTool.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gentrack.Tools.DataReplicationLoadTool.Consumers
{
    internal class FullLoadFileConsumer : IFullLoadFileConsumer
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private readonly IDatabaseService _dbService;
        private readonly ILocalCacheService _localCacheService;
        private readonly int _parallelFullLoadStreams;
        private readonly List<DatabaseMappingObject> _databaseMappingList;

        private const int FULL_LOAD_POLL_INTERVAL = 10000;


        public class TableList
        {
            public string FolderName { get; set; }
            public string ProcessName { get; set; }
        }

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
                bool isRunning = true;
                while (!cancelToken.IsCancellationRequested && !incrementalFileFound && isRunning)
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
                                    _logger.LogCritical("Delta file has been found");
                                    //throw new Exception("Delta file has been found");
                                    incrementalFileFound = true;
                                }
                                else
                                {
                                    await Uploader(fileObject);
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.LogCritical(e.ToString());
                                isRunning = false;
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

        private async Task Uploader(FileObject fileObject)
        {
            var targetDatabaseName = _databaseMappingList
                                                                            .Where(y => y.SourceDatabaseKey.Equals(fileObject.DatabaseName))
                                                                            .Select(x => x.TargetDatabaseKey).First();
            string folderList = @"..\..\..\localCachePath\validTableList.json";

            var tableRequired = 0;
            if (File.Exists(folderList))
            {
                String JSONtxt = File.ReadAllText(folderList);
                tableRequired = JsonConvert.DeserializeObject<IEnumerable<TableList>>(JSONtxt)
                .Where(s => s.ProcessName == "StagingImport")
                .Where(s => s.FolderName == fileObject.TableName)
                .Count();

                if (!tableRequired.Equals(0))
                {
                    try
                    {
                        await _dbService.BulkLoadFile(targetDatabaseName, "Junifer." + fileObject.TableName,
                        fileObject.FileKey);


                    }
                    catch (Exception e)
                    {

                        var x = e.ToString();
                    }

                }
                else
                {
                    _logger.LogInformation("Did not find table: " + fileObject.TableName + " in json list. File deleted, not imported.");
                }


                _localCacheService.MarkFileAsDone(fileObject.FileKey);
            }
        }
    }
}