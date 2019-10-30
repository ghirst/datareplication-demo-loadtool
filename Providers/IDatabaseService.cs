using System.Threading.Tasks;

namespace Gentrack.Tools.DataReplicationLoadTool.Providers
{
    public interface IDatabaseService
    {
        Task BulkLoadFile(string databaseName, string tableName, string csvFilePath);
        Task BulkLoadAndUpsertFile(string databaseName, string tableName, string csvFilePath);
    }
}
