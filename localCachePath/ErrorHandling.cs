using System;
using System.IO;

namespace Gentrack.Tools.DataReplicationLoadTool.localCachePath
{
    public class ErrorHandling
    {
        public static void LogErrorToFile(Exception e)
        {
            using StreamWriter file =
  new System.IO.StreamWriter(@"C:\Source\Repos\b2b-junifer-data-warehouse\localCachePath\errorLog\ErrorLog.txt", true);
            file.WriteLine(DateTime.UtcNow.ToString() + " (UTC) :" + e.Message.ToString());
        }
    }
}