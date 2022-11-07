using System;
using System.Management.Automation;
using System.Management.Automation.Runspaces;

namespace pscrossdemo
{
    [Cmdlet(VerbsDiagnostic.Test,"ProgressBarUsage")]
    public class TestProgressBarUsageCmdlet : PSCmdlet
    {
        [Parameter(
            Mandatory = true,
            Position = 0,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        public int MaxNumber { get; set; }

        [Parameter(
            Mandatory = false,
            Position = 1,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        public int TimeWaiting { get; set; } = 1;

        // This method gets called once for each cmdlet in the pipeline when the pipeline starts executing
        protected override void BeginProcessing()
        {
            WriteVerbose("Begin!");
        }

        // This method will be called for each input received from the pipeline to this cmdlet; if no input is received, this method is not called
        protected override void ProcessRecord()
        {
            ProgressRecord pgRec = new ProgressRecord(0, "Showing Progress", "Demo of the Progressbar");
            
            for (int i = 0; i < MaxNumber; i++)
            {
                pgRec.PercentComplete = i / MaxNumber;
                pgRec.StatusDescription = string.Format("Updated Status {0}", i);
                
                WriteProgress(pgRec);

                System.Threading.Thread.Sleep(TimeWaiting * 1000);
            }

        }

        // This method will be called once at the end of pipeline execution; if no input is received, this method is not called
        protected override void EndProcessing()
        {
            WriteVerbose("End!");
        }
    }

}
