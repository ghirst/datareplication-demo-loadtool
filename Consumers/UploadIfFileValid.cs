using Gentrack.Tools.DataReplicationLoadTool.Consumers;
using Gentrack.Tools.DataReplicationLoadTool.Providers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Gentrack.Tools.DataReplicationLoadTool.localCachePath
{
    public class UploadIfFileValid
    {
        private readonly ILogger _logger;
        private readonly List<DatabaseMappingObject> _databaseMappingList;
        private readonly IDatabaseService _dbService;

        public class TableList
        {
            public string FolderName { get; set; }
            public string ProcessName { get; set; }
        }

        public async Task Uploader(FileObject fileObject)
        {
            var targetDatabaseName = _databaseMappingList
                                                                            .Where(y => y.SourceDatabaseKey.Equals(fileObject.DatabaseName))
                                                                            .Select(x => x.TargetDatabaseKey).First();
            string folderList = @"..\..\..\localCachePath\validTableList.json";

            var tableRequired = 0;
            if (File.Exists(folderList))
            {
                String JSONtxt = File.ReadAllText(folderList);
                tableRequired = JsonConvert.DeserializeObject<IEnumerable<TableList>>(JSONtxt)
                .Where(s => s.ProcessName == "StagingImport")
                .Where(s => s.FolderName == fileObject.TableName)
                .Count();

                if (!tableRequired.Equals(0))
                {
                    try
                    {
                        await _dbService.BulkLoadFile(targetDatabaseName, "Junifer." + fileObject.TableName,
                        fileObject.FileKey);


                    }
                    catch (Exception e)
                    {

                        var x = e.ToString();
                    }

                }
                else
                {
                    _logger.LogInformation("Did not find table: " + fileObject.TableName + " in json list. File deleted, not imported.");
                }


               
            }
        }
    }
}