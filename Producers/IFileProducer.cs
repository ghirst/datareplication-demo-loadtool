using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Gentrack.Tools.DataReplicationLoadTool.Providers;

namespace Gentrack.Tools.DataReplicationLoadTool.Producers
{
    public interface IFileProducer
    {
        Task StartPolling(ConcurrentQueue<FileObject> fileQueue, CancellationToken cancelToken);
    }
}
