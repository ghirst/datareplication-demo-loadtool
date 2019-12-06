using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Gentrack.Tools.DataReplicationLoadTool.Providers
{
    public class S3Service : IRemoteEndpointService
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private IAmazonS3 _s3Client;
        private string _s3Bucket;
        private string _s3BucketPrefix;
        private string _baseCachePath;

        public S3Service(IConfigurationRoot config, ILogger<S3Service> logger, IAmazonS3 s3Client)
        {
            _config = config;
            _logger = logger;
            _s3Client = s3Client;
            _s3Bucket = _config.GetValue<string>("replicationBucket");
            _s3BucketPrefix = _config.GetValue<string>("replicationBucketPrefix");
            _baseCachePath = _config.GetValue<string>("LocalCachePath");
        }
        public async Task<List<FileObject>> GetFileList()
        {
            _logger.LogDebug($"Checking for Objects in Bucket '{_s3Bucket}'");

            ListObjectsRequest request = new ListObjectsRequest()
            {
                BucketName = _s3Bucket,
                Prefix = _s3BucketPrefix
            };

            List<FileObject> results = new List<FileObject>();

            ListObjectsResponse response;

            do
            {
                response = await _s3Client.ListObjectsAsync(request);

                results.AddRange(response.S3Objects.Select(x => GetFileObectFromS3(x)));

                request.Marker = response.NextMarker;
            }
            while(response.IsTruncated);

            return results;
        }

        public FileObject GetFileObectFromS3(S3Object inputObject)
        {
            var fileName = new FileInfo(inputObject.Key).Name;

            var tmpPathStructure = inputObject.Key.Split(new char[] { '\\', '/' });
            int folderCnt = tmpPathStructure.Length;
            var tableName = tmpPathStructure[folderCnt - 2];
            var databaseName = tmpPathStructure[folderCnt - 4];

            return new FileObject()
            {
                FileKey = inputObject.Key,
                LastModifiedDateTime = inputObject.LastModified,
                FileName = fileName,
                TableName = tableName,
                DatabaseName = databaseName
            };
        }

        public async Task<string> SyncObjectToLocalCache(string fileKey)
        {
            _logger.LogDebug("Downloading S3 object ::" + fileKey);

            var localPath = _baseCachePath + fileKey;

            var getObjectRequest = new GetObjectRequest()
            {
                BucketName = _s3Bucket,

                Key = fileKey,

            };

            using (var getObjectResponse = await _s3Client.GetObjectAsync(getObjectRequest))
            {

                if (!File.Exists(localPath))
                {
                    getObjectResponse.WriteObjectProgressEvent += Response_WriteObjectProgressEvent;

                    await getObjectResponse.WriteResponseStreamToFileAsync(localPath, true, CancellationToken.None);

                }
                else
                {
                    _logger.LogCritical("ERROR - file already exists ::" + localPath);
                }
            }
            
            _logger.LogDebug("Download Complete S3 link ::" + fileKey);
            return localPath;
        }
        private void Response_WriteObjectProgressEvent(object sender, WriteObjectProgressArgs e)
        {
            if (e.PercentDone % 10 == 0)
                _logger.LogDebug(String.Format("{3} :: Transferred: {0}/{1} - Progress: {2}%", e.TransferredBytes, e.TotalBytes, e.PercentDone, e.Key));
        }

        public async Task DeleteObject(string fileKey)
        {
            _logger.LogDebug("Delete Object ::" + fileKey);

            var deleteObjectRequest = new DeleteObjectRequest()
            {
                Key = fileKey,
                BucketName = _s3Bucket
            };

            var deleteObjectResponse = await _s3Client.DeleteObjectAsync(deleteObjectRequest);
            
        }
    }
}
