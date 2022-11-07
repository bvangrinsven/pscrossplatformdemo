using System;
using System.Data.SqlClient;
using System.Management.Automation;
using System.Management.Automation.Runspaces;

namespace pscrossdemo
{
    [Cmdlet(VerbsCommon.Copy, "SqlBulkData")]
    [OutputType(typeof (JobStatus))]
    public class CopySqlBulkDataCmdLet : PSCmdlet
    {
        private JobStatus js = new JobStatus();

        [Parameter(Mandatory = true)]
        public string SrcConnString { get; set; }

        [Parameter(Mandatory = true)]
        public string DestConnString { get; set; }

        [Parameter(Mandatory = true)]
        public string SrcTable { get; set; }

        [Parameter(Mandatory = true)]
        public string DestTable { get; set; }

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
                srcConn.Open();

                WriteVerbose("Opened the Connection");

                SqlCommand cmd =
                    new SqlCommand(string
                            .Format("SELECT * FROM [{0}]", SrcTable),
                        srcConn);
                var reader = cmd.ExecuteReader();

                WriteVerbose("Ran the Datareader");

                using (var dstConn = new SqlConnection(DestConnString))
                {
                    dstConn.Open();

                    WriteVerbose("Opened the Destination Connection");

                    var transaction = dstConn.BeginTransaction();
                    WriteVerbose("Started the Transaction");

                    using (
                        var sqlBulk =
                            new SqlBulkCopy(dstConn,
                                SqlBulkCopyOptions.KeepIdentity,
                                transaction)
                    )
                    {
                        sqlBulk.SqlRowsCopied += OnReportSqlRowsCopied;

                        WriteVerbose("Defining the BulkCopyObject");
                        sqlBulk.DestinationTableName = DestTable;

                        WriteVerbose("Starting the Data Transfer Process");
                        sqlBulk.WriteToServer (reader);
                    }

                    WriteVerbose("Completed Writing to the server");

                    transaction.Commit();
                    WriteVerbose("Transaction Complete");
                }
            }
        }

        // This method will be called once at the end of pipeline execution; if no input is received, this method is not called
        protected override void EndProcessing()
        {
            WriteVerbose("End!");
            WriteObject (js);
        }


#region "Events"

        private void OnReportSqlRowsCopied(
            object sender,
            SqlRowsCopiedEventArgs e
        )
        {
            throw new NotImplementedException();
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
