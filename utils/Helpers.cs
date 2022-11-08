using pscrossdemo.Objects;
using System.Collections.Generic;
using System.Data;

namespace pscrossdemo.utils
{
    public static class Helpers
    {
        
        public static string GetSchemaFromDB(string tableName){
            
            return string.Format("SELECT COLUMN_NAME as ColumnName, ORDINAL_POSITION as OrdinalPosition, DATA_TYPE as DataType " +
            "FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_NAME = '{0}' " + 
            "ORDER BY 2", tableName);
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

    }
}