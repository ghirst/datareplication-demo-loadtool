using System.Collections.Generic;

namespace Gentrack.Tools.DataReplicationLoadTool.Providers
{
    public interface ILocalCacheService
    {
        List<FileObject> GetUnprocessedFiles();
        void MarkFileAsDone(string localFileKey);
        void CleanUpOldFiles();
        FileObject GetFileObjectFromLocal(string localFileKey);
    }
}
