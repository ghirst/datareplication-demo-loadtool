using System;

namespace Gentrack.Tools.DataReplicationLoadTool.Providers
{
    public class FileObject
    {
        public string FileKey { get; set; }
        public DateTime LastModifiedDateTime { get; set; }
        public string FileName { get; set; }
        public string TableName { get; set; }
        public string DatabaseName { get; set; }
    }
}