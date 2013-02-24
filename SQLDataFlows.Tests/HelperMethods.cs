using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataFlows.Tests
{
    public static class HelperMethods
    {
        public static IDbConnection ExecuteStatement(this IDbConnection connection, string executeStatement, bool supressErrors = false)
        {
            if (connection.State != ConnectionState.Open)
                throw new Exception("Connection must be opened");
            try
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = executeStatement;
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                if (supressErrors)
                {
                    Console.WriteLine("Supressed error: " + ex.Message);
                }
                else
                {
                    throw;
                }
            }
            return connection;
        }

        public static int ExecuteIntScalar(this IDbConnection connection, string executeStatement)
        {
            if (connection.State != ConnectionState.Open)
                throw new Exception("Connection must be opened");

            var cmd = connection.CreateCommand();
            cmd.CommandText = executeStatement;
            return (int)cmd.ExecuteScalar();
        }

        
    }
}
