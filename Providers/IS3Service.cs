using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Gentrack.Tools.DataReplicationLoadTool.Providers
{
    public interface IS3Service
    {
        Task<List<FileObject>> GetBucketFileList();
        Task<string> SyncObjectToLocalCache(string fileKey);
        Task DeleteObject(string fileKey);
    }
}
