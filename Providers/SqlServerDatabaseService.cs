using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using GenericParsing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace Gentrack.Tools.DataReplicationLoadTool.Providers
{
    class SqlServerDatabaseService : IDatabaseService
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _config;
        private readonly string _sqlConnectionString;

        private const int DATATABLE_BATCH_SIZE = 1000;
        private const int SQL_BULKLOAD_BATCH_SIZE = 1000;
        private const int SQL_BULKLOAD_TIMEOUT = 600;
        private const int CSV_PARSER_BUFFER_SIZE = 131072;
        

        public SqlServerDatabaseService(IConfigurationRoot config, ILogger<SqlServerDatabaseService> logger)
        {
            _config = config;
            _logger = logger;
            _sqlConnectionString = _config.GetValue<string>("ConnectionStrings:SqlServer");
        }

        public async Task BulkLoadFile(string databaseName, string tableName, string csvFilePath)
        {
            //  Take care of the SQL Connection
            SqlConnectionStringBuilder sqlStringBuilder =
                new SqlConnectionStringBuilder(_sqlConnectionString) {InitialCatalog = databaseName};

            var sqlCnn = new SqlConnection(sqlStringBuilder.ConnectionString);
            sqlCnn.Open();

            //  Create a data table based on the structure of the table we want to bulk insert into
            //  Use 1=2 as a quick way to keep the table empty
            var thisAdapter = new SqlDataAdapter();
            var tableSelectSqlCmd = "Select * from {0} where 1=2";
            thisAdapter.SelectCommand = new SqlCommand(String.Format(tableSelectSqlCmd, tableName), sqlCnn);

            //  Use a DataTable as a layer in between reading the CSV file and Bulk Inserting
            //  Bulk insert does not support quoted CSV etc
            //  Using a CSV parser + a DataTable to provide a streaming interface to SqlBulkCopy is a way around this

            var thisDt = new DataTable();

            thisAdapter.Fill(thisDt);

            //  keep a copy of the data table structure
            var templateDt = thisDt.Clone();

            var batchTrack = 0;
            try
            {
                using (var csvParser = new GenericParserAdapter(csvFilePath))
                {
                    csvParser.FirstRowHasHeader = false;
                    csvParser.MaxBufferSize = CSV_PARSER_BUFFER_SIZE;

                    //  iterate through the CSV file
                    while (csvParser.Read())
                    {
                        //  write out a batch from the data table
                        if (batchTrack == DATATABLE_BATCH_SIZE)
                        {
                            await BulkInsertDataTable(sqlCnn, thisDt, tableName);

                            thisDt.Dispose();
                            thisDt = null;

                            //  reset the data table to the template
                            thisDt = templateDt.Clone();
                            batchTrack = 0;
                        }

                        //  Parse each column of the row based on tables meta data
                        //  Assumes the table structure of the Target DB and incoming CSV files match
                        //  Ensure Null values are converted to DBNull
                        var thisDr = thisDt.NewRow();

                        for (var i = 2; i < csvParser.ColumnCount; i++)
                        {
                            var targetCol = i - 2;

                            thisDr[targetCol] = ProcessColumn(csvParser[i], thisDt.Columns[targetCol].DataType);
                        }

                        thisDt.Rows.Add(thisDr);

                        batchTrack++;
                    }

                    //  Write out the final batch
                    await BulkInsertDataTable(sqlCnn, thisDt, tableName);

                    //  Cleanup
                    thisAdapter.Dispose();
                    thisDt.Dispose();
                    thisDt = null;
                }

                sqlCnn.Close();
                sqlCnn.Dispose();

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        public async Task BulkLoadAndUpsertFile(string databaseName, string tableName, string csvFilePath)
        {
            //  Take care of the SQL Connection
            SqlConnectionStringBuilder sqlStringBuilder =
                new SqlConnectionStringBuilder(_sqlConnectionString) {InitialCatalog = databaseName};

            var sqlCnn = new SqlConnection(sqlStringBuilder.ConnectionString);
            sqlCnn.Open();

            CreateCtrlTbl(sqlCnn, databaseName);

            var columnList = CreateTempTable(sqlCnn, databaseName, tableName);

            //  Create a data table based on the structure of the tmp table we want to bulk insert into
            //  Use 1=2 as a quick way to keep the table empty
            var thisAdapter = new SqlDataAdapter();
            var thisSql = "Select * from {0} where 1=2";
            thisAdapter.SelectCommand = new SqlCommand(String.Format(thisSql, "sqlTmp"), sqlCnn);

            //  Use a DataTable as a layer in between reading the CSV file and Bulk Inserting
            //  Bulk insert does not support quoted CSV etc
            //  Using a CSV parser + a DataTable to provide a streaming interface to SqlBulkCopy is a way around this
            var thisDt = new DataTable();
            thisAdapter.Fill(thisDt);

            //  keep a copy of the data table structure
            var templateDt = thisDt.Clone();

            var batchTrack = 0;
            var tmpIndexCnt = 0;
            var cntDelLines = 0;
            var cntInsLines = 0;
            var cntUpdLines = 0;

            try
            {
                using (var parser = new GenericParserAdapter(csvFilePath))
                {
                    parser.FirstRowHasHeader = false;
                    parser.MaxBufferSize = CSV_PARSER_BUFFER_SIZE;


                    //  iterate through the CSV file
                    while (parser.Read())
                    {
                        //  write out a batch from the data table
                        if (batchTrack == DATATABLE_BATCH_SIZE)
                        {
                            //Console.WriteLine("Task ID:::" + taskId + "(" + tableName + ")" + "Writing out a batch of " + batchTrack);

                            await BulkInsertDataTable(sqlCnn, thisDt, "sqlTmp");

                            thisDt.Dispose();
                            thisDt = null;

                            //  reset the data table to the template
                            thisDt = templateDt.Clone();

                            batchTrack = 0;
                        }

                        //  Parse each column of the row based on tables meta data
                        //  Assumes the table structure of the Target DB and incoming CSV files match
                        //  Ensure Null values are converted to DBNull
                        var thisDr = thisDt.NewRow();

                        //  populate additional key to track records uniquely in the sqlTmp table
                        thisDr[0] = tmpIndexCnt;

                        for (var i = 0; i < parser.ColumnCount; i++)
                        {
                            thisDr[i + 1] = ProcessColumn(parser[i], thisDt.Columns[i + 1].DataType);
                        }

                        thisDt.Rows.Add(thisDr);
                        batchTrack++;
                        tmpIndexCnt++;

                        // keep track of record counts by operation
                        if (thisDr[1].Equals("I"))
                        {
                            cntInsLines++;
                        }
                        else if (thisDr[1].Equals("U"))
                        {
                            cntUpdLines++;
                        }
                        else if (thisDr[1].Equals("D"))
                        {
                            cntDelLines++;
                        }
                        else
                        {
                            //Should never get here?!
                        }
                    }

                    //  Write out the final batch
                    await BulkInsertDataTable(sqlCnn, thisDt, "sqlTmp");

                    //  Cleanup
                    thisAdapter.Dispose();
                    thisDt.Dispose();
                    thisDt = null;
                }

                // Do a form of UPSERT based on the incoming file having 
                // I = insert
                // U = update
                // D = delete


                // first delete records matching any operation in target table
                var delCmd =
                    "DELETE TARGET FROM dbo.{0} as TARGET INNER JOIN dbo.sqlTmp SOURCE on TARGET.{1} = SOURCE.{2} and SOURCE.[oper] in ('I','U',\'D\')";
                await using (var cmd = new SqlCommand(String.Format(delCmd, tableName, columnList[0], columnList[0]),
                    sqlCnn))
                {
                    cmd.CommandTimeout = 120;
                    cmd.ExecuteNonQuery();
                }

                // now insert U and I rows only
                var insertCmd =
                    "INSERT INTO dbo.{0} ({1}) SELECT {2} FROM sqlTmp SOURCE where SOURCE.oper in ('I', 'U') and SOURCE.[index] = (select Max(SOURCE2.[index]) from sqlTmp SOURCE2 where SOURCE2.id = SOURCE.id)";
                await using (var cmd =
                    new SqlCommand(
                        String.Format(insertCmd, tableName, string.Join(",", columnList), string.Join(",", columnList)),
                        sqlCnn))
                {
                    cmd.CommandTimeout = 120;
                    cmd.ExecuteNonQuery();
                }

                // finally insert a control line
                var ctrlInsertCmd =
                    "INSERT INTO dbo.repLog ([file],[table],[delCnt],[insCnt],[updCnt]) VALUES ('{0}','{1}','{2}','{3}','{4}')";
                await using (var cmd = new SqlCommand(String.Format(ctrlInsertCmd, Path.GetFileName(csvFilePath), tableName, cntDelLines, cntInsLines, cntUpdLines), sqlCnn))
                {
                    cmd.CommandTimeout = 120;
                    cmd.ExecuteNonQuery();
                }
                

                sqlCnn.Close();
                sqlCnn.Dispose();

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }


            //var endDttm = DateTime.Now;
            //var span = endDttm.Subtract(startDttm);
            //Console.WriteLine(endDttm + ":: Finished Task ID:::" + taskId + "(" + tableName + ")::: " + "Duration (minutes): " + span.TotalMinutes);
        }

        private async Task BulkInsertDataTable(SqlConnection sqlCnn, DataTable sourceData, string targetTable)
        {
            using (var sqlBulk = new SqlBulkCopy(sqlCnn))
            {
                sqlBulk.DestinationTableName = targetTable;
                sqlBulk.BulkCopyTimeout = SQL_BULKLOAD_TIMEOUT;
                sqlBulk.BatchSize = SQL_BULKLOAD_BATCH_SIZE;
                await sqlBulk.WriteToServerAsync(sourceData);
            }
        }

        private string[] CreateTempTable(SqlConnection sqlCnn, string databaseName, string sourceTableName)
        {

            //  Create a tmp table called "sqlTmp" matching the target table structure with additional 2 columns
            //  to track operations
            var targetServer = new Server(new ServerConnection(sqlCnn));
            var sourceTable = targetServer.Databases[databaseName].Tables[sourceTableName];


            if (targetServer.Databases[databaseName].Tables["sqlTmp"] != null)
            {
                targetServer.Databases[databaseName].Tables["sqlTmp"].Drop();
            }

            var tempTable = new Table(targetServer.Databases[databaseName], "sqlTmp");

            var tempIndex = new Column(tempTable, "index") { DataType = DataType.Int };

            var tempCol1 = new Column(tempTable, "oper") { DataType = DataType.NVarChar(100) };
            var tempCol2 = new Column(tempTable, "replicationDttm") { DataType = DataType.DateTime };

            tempTable.Columns.Add(tempIndex, 0);
            tempTable.Columns.Add(tempCol1, 1);
            tempTable.Columns.Add(tempCol2, 2);

            //  keep track of the source columns for Insert statements later
            var columnList = new String[sourceTable.Columns.Count];

            // Duplicate the table with additional columns
            for (var z = 0; z < sourceTable.Columns.Count; z++)
            {
                var tempColumn = new Column(tempTable, sourceTable.Columns[z].Name)
                {
                    DataType = sourceTable.Columns[z].DataType
                };

                tempTable.Columns.Add(tempColumn, z + 3);

                columnList[z] = sourceTable.Columns[z].Name;
            }

            tempTable.Create();

            return columnList;
        }

        private void CreateCtrlTbl(SqlConnection sqlCnn, string databaseName)
        {
            var targetServer = new Server(new ServerConnection(sqlCnn));

            if (!targetServer.Databases[databaseName].Tables.Contains("repLog", "dbo"))
            {
                var repLog = new Table(targetServer.Databases[databaseName], "repLog");

                var col1 = new Column(repLog, "id")
                {
                    DataType = DataType.Int,
                    Identity = true,
                    IdentitySeed = 1,
                    IdentityIncrement = 1
                };

                repLog.Columns.Add(col1);
                repLog.Columns.Add(new Column(repLog, "file", DataType.NVarChar(100)));
                repLog.Columns.Add(new Column(repLog, "table", DataType.NVarChar(100)));
                repLog.Columns.Add(new Column(repLog, "delCnt", DataType.Int));
                repLog.Columns.Add(new Column(repLog, "insCnt", DataType.Int));
                repLog.Columns.Add(new Column(repLog, "updCnt", DataType.Int));

                repLog.Create();
            }
        }

        private dynamic ProcessColumn(dynamic colIn, Type outType)
        {
            dynamic colOut;
            if (outType == typeof(int))
            {
                int x;
                if (int.TryParse(colIn, out x))
                    colOut = x;
                else
                    colOut = DBNull.Value;
            }
            else if (outType == typeof(Int16))
            {
                Int16 x;
                if (Int16.TryParse(colIn, out x))
                    colOut = x;
                else
                    colOut = DBNull.Value;
            }
            else if (outType == typeof(Int32))
            {
                Int32 x;
                if (Int32.TryParse(colIn, out x))
                    colOut = x;
                else
                    colOut = DBNull.Value;
            }
            else if (outType == typeof(Int64))
            {
                Int64 x;
                if (Int64.TryParse(colIn, out x))
                    colOut = x;
                else
                    colOut = DBNull.Value;
            }
            else if (outType == typeof(DateTime))
            {
                DateTime x;
                if (DateTime.TryParse(colIn, out x))
                    colOut = x;
                else
                    colOut = DBNull.Value;
            }
            else if (outType == typeof(DateTime))
            {
                DateTime x;
                if (DateTime.TryParse(colIn, out x))
                    colOut = x;
                else
                    colOut = DBNull.Value;
            }
            else if (outType == typeof(Char))
            {
                Char x;
                if (Char.TryParse(colIn, out x))
                    colOut = x;
                else
                    colOut = DBNull.Value;
            }
            else if (outType == typeof(Decimal))
            {
                Decimal x;
                if (Decimal.TryParse(colIn, out x))
                    colOut = x;
                else
                    colOut = DBNull.Value;
            }
            else
            {
                colOut = colIn;
            }

            return colOut;
        }
    }
}
