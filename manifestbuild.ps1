$manifestSplat = @{
  Path              = ".\pscrossdemo.psd1"
  Author            = 'Kevin Marquette'
  NestedModules     = @('bin\Debug\netstandard2.0\pscrossdemo.dll')
  RootModule        = "$module.psm1"
  FunctionsToExport = @('Copy-SqlBulkData', 'Test-ProgressBarUsage', 'Test-SampleCmdlet')
}
New-ModuleManifest @manifestSplat