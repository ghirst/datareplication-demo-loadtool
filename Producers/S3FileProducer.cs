using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Gentrack.Tools.DataReplicationLoadTool.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Gentrack.Tools.DataReplicationLoadTool.Producers
{
    class S3FileProducer : IFileProducer
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private readonly IS3Service _s3Service;
        private readonly ILocalCacheService _localCacheService;

        private const int S3_POLL_INTERVAL = 10000;
        public S3FileProducer(IConfigurationRoot config, ILogger<S3FileProducer> logger, IS3Service s3Service, ILocalCacheService localCacheService)
        {
            _config = config;
            _logger = logger;
            _s3Service = s3Service;
            _localCacheService = localCacheService;
        }

        public async Task StartPolling(ConcurrentQueue<FileObject> fileQueue, CancellationToken cancelToken)
        {
            _logger.LogInformation("Starting S3 File Producer");

            _logger.LogInformation("Checking local cache for unprocessed files...");

            // first check local cache for any incomplete files
            var incompleteFileList = _localCacheService.GetUnprocessedFiles();

            // Full load files first
            foreach (var file in incompleteFileList.Where(z => z.FileName.StartsWith("LOAD")).OrderBy(x => x.FileName))
            {
                _logger.LogInformation($"Adding incomplete file to queue :: {file.FileKey}");

                fileQueue.Enqueue(file);

            }
            // Delta files
            foreach (var file in incompleteFileList.Where(z => !z.FileName.StartsWith("LOAD")).OrderBy(x => x.FileName))
            {
                _logger.LogInformation($"Adding incomplete file to queue :: {file.FileKey}");

                fileQueue.Enqueue(file);

            }

            while (!cancelToken.IsCancellationRequested)
            {
                _logger.LogInformation("Polling bucket...");

                // Now check actual source endpoint
                var fileList = await _s3Service.GetBucketFileList();

                // first add any files for full load
                foreach (var file in fileList.Where(z => z.FileName.StartsWith("LOAD")).OrderBy(x => x.FileName))
                {
                    _logger.LogInformation($"Downloading file to local cache :: {file.FileKey}");

                    var localKey = await _s3Service.SyncObjectToLocalCache(file.FileKey);

                    _logger.LogDebug($"Adding file to queue :: {localKey}");

                    fileQueue.Enqueue(_localCacheService.GetFileObjectFromLocal(localKey));
                    
                    _logger.LogDebug($"Delete object on s3 bucket :: {file.FileKey}");

                    await _s3Service.DeleteObject(file.FileKey);

                }

                // now add any Delta files found
                foreach (var file in fileList.Where(z => !z.FileName.StartsWith("LOAD")).OrderBy(x => x.FileName))
                {
                    _logger.LogInformation($"Downloading file to local cache :: {file.FileKey}");

                    var localKey = await _s3Service.SyncObjectToLocalCache(file.FileKey);

                    _logger.LogDebug($"Adding file to queue :: {localKey}");

                    fileQueue.Enqueue(_localCacheService.GetFileObjectFromLocal(localKey));

                    _logger.LogDebug($"Delete object on s3 bucket :: {file.FileKey}");

                    await _s3Service.DeleteObject(file.FileKey);

                }

                await Task.Delay(S3_POLL_INTERVAL, cancelToken);

            }

            if (cancelToken.IsCancellationRequested)
            {
                _logger.LogInformation("Stopping S3 File Producer");
            }
        }

    }
}
