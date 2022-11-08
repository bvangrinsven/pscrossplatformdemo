using System;
using System.Data;

namespace pscrossdemo.Objects
{
  public class SchemaProp
  {
    public string ColumnName { get; set; } = string.Empty;
    public int OrdinalPosition { get; set; } = 0;
    public string DataType { get; set; } = string.Empty;


    public static SchemaProp CreateObject(DataRow dataRow)
    {
        SchemaProp result  = new SchemaProp();

        result.ColumnName = dataRow["ColumnName"].ToString();
        result.OrdinalPosition = Convert.ToInt32(dataRow["OrdinalPosition"]);
        result.DataType = dataRow["DataType"].ToString();

        return result;
    }

  }
}