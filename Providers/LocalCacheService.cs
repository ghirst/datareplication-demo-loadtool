using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Gentrack.Tools.DataReplicationLoadTool.Providers
{
    class LocalCacheService : ILocalCacheService
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private readonly string _baseCachePath;

        private const int MAX_FILE_RETENTION_PERIOD_DAYS = -90;
        public LocalCacheService(IConfigurationRoot config, ILogger<S3Service> logger)
        {
            _config = config;
            _logger = logger;
            _baseCachePath = _config.GetValue<string>("LocalCachePath");
        }

        public List<FileObject> GetUnprocessedFiles()
        {
            var unprocessedFileList = new List<FileObject>();

            var directory = new DirectoryInfo(_baseCachePath);

            unprocessedFileList.AddRange(directory.GetFiles("*.csv", SearchOption.AllDirectories).Select(x => GetFileObectFromLocal(x)));

            return unprocessedFileList;
        }

        public void MarkFileAsDone(string localFileKey)
        {
            var originalFileName = Path.GetFileName(localFileKey);
            var originalPathWithoutFileName = Path.GetDirectoryName(localFileKey);
            var doneFileName = originalFileName + ".done";
            var doneFileKey = Path.Combine(originalPathWithoutFileName, doneFileName);

            try
            {
                File.Move(localFileKey, doneFileKey);
            }
            catch (Exception E)
            {
                throw E;
            }

        }

        public void CleanUpOldFiles()
        {
            _logger.LogDebug("Cleaning up old files in local cache");

            var directory = new DirectoryInfo(_baseCachePath);
            var retentionPeriodCutoffDttm = DateTime.Now.AddDays(MAX_FILE_RETENTION_PERIOD_DAYS);

            var filesToRemove = directory.GetFiles().Where(z => z.Extension.Equals("done") && z.LastWriteTime <= retentionPeriodCutoffDttm);

            _logger.LogDebug($"Found {filesToRemove.Count()} old files to remove");

            foreach (var file in filesToRemove)
            {
                _logger.LogDebug($"Deleting old file :: {file.Name}");
                file.Delete();
            }
        }

        private FileObject GetFileObectFromLocal(FileInfo localFileObject)
        {
            var tmpPathStructure = localFileObject.FullName.Split(new char[] { '\\', '/' });
            int folderCnt = tmpPathStructure.Length;
            var tableName = tmpPathStructure[folderCnt - 2];
            var databaseName = tmpPathStructure[folderCnt - 4] + "_repl";

            return new FileObject()
            {
                FileKey = localFileObject.FullName,
                LastModifiedDateTime = localFileObject.LastWriteTime,
                FileName = localFileObject.Name,
                TableName = tableName,
                DatabaseName = databaseName
            };
        }

        public FileObject GetFileObjectFromLocal(string localFileKey)
        {
            var localFileObject = new FileInfo(localFileKey);

            var tmpPathStructure = localFileKey.Split(new char[] { '\\', '/' });
            int folderCnt = tmpPathStructure.Length;
            var tableName = tmpPathStructure[folderCnt - 2];
            var databaseName = tmpPathStructure[folderCnt - 4] + "_repl";

            return new FileObject()
            {
                FileKey = localFileObject.FullName,
                LastModifiedDateTime = localFileObject.LastWriteTime,
                FileName = localFileObject.Name,
                TableName = tableName,
                DatabaseName = databaseName
            };
        }
    }
}
