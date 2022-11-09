using pscrossdemo.Objects;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace pscrossdemo.utils
{
  public static class Helpers
  {

    public static string GetSchemaFromDBSql(string tableName)
    {

      return string.Format("SELECT COLUMN_NAME as ColumnName, ORDINAL_POSITION as OrdinalPosition, DATA_TYPE as DataType " +
      "FROM INFORMATION_SCHEMA.COLUMNS " +
      "WHERE TABLE_NAME = '{0}' " +
      "ORDER BY 2", tableName);
    }

    public static string GetRowCountSql(string tableName)
    {
      return string
              .Format("SELECT CAST(p.rows AS float) " +
              "FROM sys.tables AS tbl " +
              "INNER JOIN sys.indexes AS idx ON idx.object_id = tbl.object_id and idx.index_id < 2 " +
              "INNER JOIN sys.partitions AS p ON p.object_id=CAST(tbl.object_id AS int) AND p.index_id=idx.index_id " +
              "WHERE ((tbl.name='{0}' AND SCHEMA_NAME(tbl.schema_id)='dbo'))",
              tableName);
    }

    public static string TruncateTableSql(string tableName)
    {
      return string.Format("TRUNCATE TABLE [{0}]", tableName);
    }


    public static List<SchemaProp> DataTableSchemaProp(DataTable dtConvert)
    {
      List<SchemaProp> result = new List<SchemaProp>();

      foreach (DataRow dr in dtConvert.Rows)
      {
        result.Add(SchemaProp.CreateObject(dr));
      }

      return result;
    }

    public static string GetPullAllSql(string SrcTableName, List<SchemaProp> srcProps, List<SchemaProp> destProps, bool UseDefault = false)
    {
      StringBuilder result = new StringBuilder();

      result.Append("SELECT ");

      foreach (SchemaProp dstCol in destProps)
      {
        if (srcProps.Exists(x => x.ColumnName == dstCol.ColumnName))
        {
          result.AppendFormat("[{0}], ", dstCol.ColumnName);
        }
        else
        {
          if (UseDefault)
          {
            switch (dstCol.DataType)
            {

              case "bigint":
              case "numeric":
              case "bit":
              case "smallint":
              case "decimal":
              case "smallmoney":
              case "int":
              case "tinyint":
              case "money":
              case "float":
              case "real":
                result.AppendFormat("0 as [{0}], ", dstCol.ColumnName);
                break;

              case "char":
              case "varchar":
              case "text":
              case "nchar":
              case "nvarchar":
              case "ntext":
              default:
                result.AppendFormat("'' as [{0}], ", dstCol.ColumnName);
                break;
            }
          }
        }
      }

      result.AppendFormat("FROM [{0}] \r\n", SrcTableName);

      return result.ToString().Replace(", FROM", " FROM");
    }

  }
}