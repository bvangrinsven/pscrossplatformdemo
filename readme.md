# Powershell Standard Modules Playground

Used for cross platform demoing of features and creation of stuff.

## CopySqlBulkDataCmdLet

```powershell
Copy-SqlBulkData -SrcConnString "Server=localhost;Database=lists;User Id=sa;Password=mssql1Ipw;" -DestConnString "Server=localhost;Database=dest;User Id=sa;Password=mssql1Ipw;" -SrcTable "tblList" -DestTable "tblList" -Verbose -TruncateDestTable -MapColumns


```





## Setting up the development project

```powershell
dotnet new -i Microsoft.PowerShell.Standard.Module.Template
dotnet new psmodule
dotnet build
Import-Module "bin\Debug\netstandard2.0\$module.dll"
Get-Module $module

```
