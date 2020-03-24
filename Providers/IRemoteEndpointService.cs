using System.Collections.Generic;
using System.Threading.Tasks;

namespace Gentrack.Tools.DataReplicationLoadTool.Providers
{
    public interface IRemoteEndpointService
    {
        Task<List<FileObject>> GetFileList();

        Task<string> SyncObjectToLocalCache(string fileKey);

        Task DeleteObject(string fileKey);
    }
}