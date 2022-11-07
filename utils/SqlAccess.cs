using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;

namespace pscrossdemo.utils
{
    public static class SqlAccess
    {
        public static SqlCommand
        GetCommand(
            this SqlConnection Connection,
            string CommandText,
            IEnumerable<SqlParameter> Parameters = null,
            System.Data.CommandType CommandType = System.Data.CommandType.Text
        )
        {
            if (Connection == null)
            {
                throw new ArgumentNullException("Connection is null");
            }
            if (string.IsNullOrEmpty(CommandText))
            {
                throw new ArgumentNullException("CommandText is null");
            }
            SqlCommand Command = Connection.CreateCommand();
            Command.CommandTimeout = 3600;
            Command.CommandText = CommandText;
            Command.CommandType = CommandType;
            if (Parameters != null)
            {
                foreach (SqlParameter Parameter in Parameters)
                {
                    Command.Parameters.Add (Parameter);
                }
            }
            return Command;
        }

        public static SqlCommand
        GetCommand(
            this SqlTransaction Transaction,
            string CommandText,
            IEnumerable<SqlParameter> Parameters = null,
            System.Data.CommandType CommandType = System.Data.CommandType.Text
        )
        {
            if (Transaction == null)
            {
                throw new ArgumentNullException("Transaction is null");
            }
            if (string.IsNullOrEmpty(CommandText))
            {
                throw new ArgumentNullException("CommandText is null");
            }
            SqlCommand Command = Transaction.Connection.CreateCommand();
            Command.CommandTimeout = 3600;
            Command.CommandText = CommandText;
            Command.CommandType = CommandType;
            if (Parameters != null)
            {
                foreach (SqlParameter Parameter in Parameters)
                {
                    Command.Parameters.Add (Parameter);
                }
            }
            return Command;
        }

        public static SqlDataReader
        GetDataReader(
            this SqlCommand Command,
            System.Data.CommandBehavior
            CommandBehavior = System.Data.CommandBehavior.Default
        )
        {
            SqlDataReader sqlDataReader;
            if (Command == null)
            {
                throw new ArgumentNullException("Command is null");
            }
            if (Command.Connection == null)
            {
                throw new ArgumentNullException("Connection is null");
            }
            using (Command)
            {
                SqlConnection Connection = Command.Connection;
                if (Connection.State != ConnectionState.Open)
                {
                    Connection.Open();
                }
                sqlDataReader = Command.ExecuteReader(CommandBehavior);
            }
            return sqlDataReader;
        }

        public static SqlDataReader
        GetDataReader(
            this SqlConnection Connection,
            string CommandText,
            IEnumerable<SqlParameter> Parameters = null,
            System.Data.CommandType CommandType = System.Data.CommandType.Text,
            System.Data.CommandBehavior
            CommandBehavior = System.Data.CommandBehavior.Default
        )
        {
            SqlCommand Command =
                Connection.GetCommand(CommandText, Parameters, CommandType);
            return Command.GetDataReader(CommandBehavior);
        }

        public static DataTable
        GetDataTable(this SqlCommand Command, string TableName = "")
        {
            DataTable dataTable;
            if (Command == null)
            {
                throw new ArgumentNullException("Command is null");
            }
            if (Command.Connection == null)
            {
                throw new ArgumentNullException("Command.Connection is null");
            }
            using (Command.Connection)
            {
                using (Command)
                {
                    using (SqlDataAdapter Adapter = new SqlDataAdapter(Command))
                    {
                        DataTable dt = new DataTable(TableName);
                        Adapter.Fill (dt);
                        dataTable = dt;
                    }
                }
            }
            return dataTable;
        }

        public static DataTable
        GetDataTable(
            this SqlConnection Connection,
            string CommandText,
            IEnumerable<SqlParameter> Parameters = null,
            System.Data.CommandType CommandType = System.Data.CommandType.Text,
            string TableName = ""
        )
        {
            SqlCommand Command =
                Connection.GetCommand(CommandText, Parameters, CommandType);
            return Command.GetDataTable(TableName);
        }

        public static int GetNonQuery(this SqlCommand Command)
        {
            int num;
            if (Command == null)
            {
                throw new ArgumentNullException("Command is null");
            }
            if (Command.Connection == null)
            {
                throw new ArgumentNullException("Connection is null");
            }
            using (Command)
            {
                using (SqlConnection Connection = Command.Connection)
                {
                    if (Connection.State != ConnectionState.Open)
                    {
                        Connection.Open();
                    }
                    num = Command.ExecuteNonQuery();
                }
            }
            return num;
        }

        public static object
        GetNonQuery(
            this SqlConnection Connection,
            string CommandText,
            IEnumerable<SqlParameter> Parameters = null,
            System.Data.CommandType CommandType = System.Data.CommandType.Text
        )
        {
            SqlCommand Command =
                Connection.GetCommand(CommandText, Parameters, CommandType);
            return Command.GetNonQuery();
        }

        public static object GetScalar(this SqlCommand Command)
        {
            object obj;
            if (Command == null)
            {
                throw new ArgumentNullException("Command is null");
            }
            if (Command.Connection == null)
            {
                throw new ArgumentNullException("Connection is null");
            }
            using (Command)
            {
                using (SqlConnection Connection = Command.Connection)
                {
                    if (Connection.State != ConnectionState.Open)
                    {
                        Connection.Open();
                    }
                    obj = Command.ExecuteScalar();
                }
            }
            return obj;
        }

        public static object
        GetScalar(
            this SqlConnection Connection,
            string CommandText,
            IEnumerable<SqlParameter> Parameters = null,
            System.Data.CommandType CommandType = System.Data.CommandType.Text
        )
        {
            SqlCommand Command =
                Connection.GetCommand(CommandText, Parameters, CommandType);
            return Command.GetScalar();
        }

        public static T GetScalar<T>(this SqlCommand Command)
        {
            T t;
            if (Command == null)
            {
                throw new ArgumentNullException("Command is null");
            }
            using (Command)
            {
                if (Command.Connection == null)
                {
                    throw new ArgumentNullException("Connection is null");
                }
                using (SqlConnection Connection = Command.Connection)
                {
                    if (Connection.State != ConnectionState.Open)
                    {
                        Connection.Open();
                    }
                    object Value = Command.ExecuteScalar();
                    if (
                        object.ReferenceEquals(Value, DBNull.Value) ||
                        Value == null
                    )
                    {
                        t = default(T);
                    }
                    else if (
                        object.ReferenceEquals(typeof (T), Value.GetType()) ||
                        typeof (T).IsAssignableFrom(Value.GetType())
                    )
                    {
                        t = (T) Value;
                    }
                    else
                    {
                        t =
                            (
                            !typeof (T).IsGenericType ||
                            !(
                            typeof (T).GetGenericTypeDefinition() ==
                            typeof (Nullable<>)
                            )
                                ? (T) Convert.ChangeType(Value, typeof (T))
                                : (T)
                                Convert
                                    .ChangeType(Value,
                                    typeof (T).GetGenericArguments()[0])
                            );
                    }
                }
            }
            return t;
        }

        public static T
        GetScalar<T>(
            this SqlConnection Connection,
            string CommandText,
            IEnumerable<SqlParameter> Parameters = null,
            System.Data.CommandType CommandType = System.Data.CommandType.Text
        )
        {
            SqlCommand Command =
                Connection.GetCommand(CommandText, Parameters, CommandType);
            return Command.GetScalar<T>();
        }

        public static SqlConnection GetSQLConnection(string connectionString)
        {
            return new SqlConnection(connectionString);
        }
    }
}
