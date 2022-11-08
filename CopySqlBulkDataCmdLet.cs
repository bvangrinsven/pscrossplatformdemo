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
    }

    // This method will be called for each input received from the pipeline to this cmdlet; if no input is received, this method is not called
    protected override void ProcessRecord()
    {
      using (var srcConn = new SqlConnection(SrcConnString))
      {
        js.SourceRowCount = GetRowCount(srcConn, SrcTable);

        WriteVerbose("Opened the Connection");

        var reader = srcConn.GetDataReader(string.Format("SELECT * FROM [{0}]", SrcTable));

        WriteVerbose("Ran the Datareader");

        using (var dstConn = new SqlConnection(DestConnString))
        {
          dstConn.Open();

          WriteVerbose("Opened the Destination Connection");

          if (TruncateDestTable)
          {
            WriteVerbose(" -- Clearing Destination Table Data");
            TruncateTable(dstConn, DestTable);
            WriteVerbose(" -- Table Cleared");
          }

          var transaction = dstConn.BeginTransaction();
          WriteVerbose("Started the Transaction");

          using (var sqlBulk = new SqlBulkCopy(dstConn, SqlBulkCopyOptions.KeepIdentity, transaction))
          {
            sqlBulk.SqlRowsCopied += OnReportSqlRowsCopied;
            sqlBulk.BulkCopyTimeout = 3600;
            sqlBulk.NotifyAfter = (int)((double)js.SourceRowCount * 0.1);
            sqlBulk.BatchSize = (int)((double)js.SourceRowCount * 0.001);

            WriteVerbose("Defining the BulkCopyObject");
            sqlBulk.DestinationTableName = string.Format("[{0}]", DestTable);

            if (MapColumns)
            {
              CreateColumnMapping(srcConn, dstConn, SrcTable, DestTable, sqlBulk.ColumnMappings);
            }

            WriteVerbose("Starting the Data Transfer Process");
            sqlBulk.WriteToServer(reader);
          }

          WriteVerbose("Completed Writing to the server");

          transaction.Commit();
          WriteVerbose("Transaction Complete");

          js.DestinationRowCount = GetRowCount(dstConn, DestTable);
        }

        reader.Close();
      }
    }

    // This method will be called once at the end of pipeline execution; if no input is received, this method is not called
    protected override void EndProcessing()
    {
      WriteVerbose("End!");
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

    private long GetRowCount(SqlConnection conn, string tableName)
    {
      return conn
          .GetScalar<long>(string
              .Format("SELECT CAST(p.rows AS float) " +
              "FROM sys.tables AS tbl " +
              "INNER JOIN sys.indexes AS idx ON idx.object_id = tbl.object_id and idx.index_id < 2 " +
              "INNER JOIN sys.partitions AS p ON p.object_id=CAST(tbl.object_id AS int) AND p.index_id=idx.index_id " +
              "WHERE ((tbl.name='{0}' AND SCHEMA_NAME(tbl.schema_id)='dbo'))",
              tableName));
    }

    private void TruncateTable(SqlConnection conn, string tableName)
    {
      conn.GetNonQuery(string.Format("TRUNCATE TABLE [{0}}]", tableName));
    }

    private void CreateColumnMapping(SqlConnection srcConn, SqlConnection destConn, string srcTableName, string destTableName, SqlBulkCopyColumnMappingCollection sqlBulkCopyColumnMappingCollection)
    {

      List<SchemaProp> srcCols = Helpers.DataTableSchemaProp(srcConn.GetDataTable(Helpers.GetSchemaFromDB(srcTableName)));
      List<SchemaProp> destCols = Helpers.DataTableSchemaProp(destConn.GetDataTable(Helpers.GetSchemaFromDB(destTableName)));

      foreach (SchemaProp srcCol in srcCols)
      {
        if (destCols.FindIndex(x => x.ColumnName == srcCol.ColumnName) > 0)
        {
          sqlBulkCopyColumnMappingCollection.Add(srcCol.ColumnName, srcCol.ColumnName);
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
  }
}
