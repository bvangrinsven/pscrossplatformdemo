using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using pscrossdemo.Objects;
using pscrossdemo.utils;

namespace pscrossdemo
{
    [Cmdlet(VerbsCommon.Copy, "SqlBulkData")]
    [OutputType(typeof(JobStatus))]
    public class CopySqlBulkDataCmdLet : PSCmdlet
    {
        private JobStatus js = new JobStatus();
        private SqlConnection srcConn;
        private SqlConnection dstConn;

        private List<SchemaProp> srcCols;
        private List<SchemaProp> destCols;

        private ProgressRecord pgRec = new ProgressRecord(0, "Copying Data", "Starting Process");

        [Parameter(Mandatory = true)]
        public string SrcConnString { get; set; }

        [Parameter(Mandatory = true)]
        public string DestConnString { get; set; }

        [Parameter(Mandatory = true)]
        public string SrcTable { get; set; }

        [Parameter(Mandatory = true)]
        public string DestTable { get; set; }

        [Parameter(Mandatory = false)]
        public SwitchParameter TruncateDestTable { get; set; }

        [Parameter(Mandatory = false)]
        public SwitchParameter MapColumns { get; set; }


        // This method gets called once for each cmdlet in the pipeline when the pipeline starts executing
        protected override void BeginProcessing()
        {
            WriteVerbose("Initializing the transfer objects");
            js.SourceTable = SrcTable;
            js.DestinationTable = DestTable;

            WriteVerbose("Initilize Connections to SQL Servers");
            srcConn = new SqlConnection(SrcConnString);
            dstConn = new SqlConnection(DestConnString);

            WriteVerbose("Get the Number of Rows In Source Table");
            js.SourceRowCount = srcConn.GetScalar<long>(Helpers.GetRowCountSql(SrcTable));

            WriteVerbose("Get the Schemas from the source and destination tables");
            srcCols = Helpers.DataTableSchemaProp(srcConn.GetDataTable(Helpers.GetSchemaFromDBSql(SrcTable)));
            destCols = Helpers.DataTableSchemaProp(dstConn.GetDataTable(Helpers.GetSchemaFromDBSql(DestTable)));

            if (TruncateDestTable)
            {
                WriteVerbose("Clearing Destination Table Data");
                dstConn.GetNonQuery(Helpers.TruncateTableSql(DestTable));
                WriteVerbose("Table Cleared");
            }

            js.SourcePullSql = Helpers.GetPullAllSql(SrcTable, srcCols, destCols);

        }

        // This method will be called for each input received from the pipeline to this cmdlet; if no input is received, this method is not called
        protected override void ProcessRecord()
        {

            WriteVerbose("Ran the Datareader");
            var reader = srcConn.GetDataReader(js.SourcePullSql);

            using (SqlConnection destConn = dstConn)
            {
                destConn.Open();
                var transaction = destConn.BeginTransaction();
                WriteVerbose("Started the Transaction");

                using (var sqlBulk = new SqlBulkCopy(destConn, SqlBulkCopyOptions.KeepIdentity, transaction))
                {
                    sqlBulk.SqlRowsCopied += OnReportSqlRowsCopied;
                    sqlBulk.BulkCopyTimeout = 3600;
                    sqlBulk.NotifyAfter = (int)((double)js.SourceRowCount * 0.1);
                    sqlBulk.BatchSize = (int)((double)js.SourceRowCount * 0.001);

                    WriteVerbose("Defining the BulkCopyObject");
                    sqlBulk.DestinationTableName = string.Format("[{0}]", DestTable);

                    if (MapColumns)
                    {
                        CreateColumnMapping(sqlBulk.ColumnMappings);
                    }

                    WriteVerbose("Starting the Data Transfer Process");
                    sqlBulk.WriteToServer(reader);
                }

                WriteVerbose("Completed Writing to the server");

                transaction.Commit();
                WriteVerbose("Transaction Complete");

                js.DestinationRowCount = dstConn.GetScalar<long>(Helpers.GetRowCountSql(DestTable));
            }
            reader.Close();

        }

        // This method will be called once at the end of pipeline execution; if no input is received, this method is not called
        protected override void EndProcessing()
        {
            WriteVerbose("Complete");
            WriteObject(js);
        }


        #region "Events"
        private void OnReportSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            pgRec.StatusDescription = string.Format("Row Count: {0} / {1}", e.RowsCopied, js.SourceRowCount);

            pgRec.PercentComplete = (int)(e.RowsCopied * 100 / js.SourceRowCount);

            WriteProgress(pgRec);
        }

        #endregion

        #region "Helper Functions"

        private void CreateColumnMapping(SqlBulkCopyColumnMappingCollection sqlBulkCopyColumnMappingCollection)
        {
            foreach (SchemaProp dstCol in destCols)
            {
                if (srcCols.FindIndex(x => x.ColumnName == dstCol.ColumnName) > 0)
                {
                    sqlBulkCopyColumnMappingCollection.Add(dstCol.ColumnName, dstCol.ColumnName);
                }
            }
        }

        #endregion

    }

    public class JobStatus
    {
        public int Result { get; set; } = 0;

        public string SourceTable { get; set; } = string.Empty;

        public long SourceRowCount { get; set; } = 0;

        public string DestinationTable { get; set; } = string.Empty;

        public long DestinationRowCount { get; set; } = 0;

        public string SourcePullSql { get; set; } = string.Empty;

    }
}
