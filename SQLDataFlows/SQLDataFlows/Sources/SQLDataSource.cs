using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataFlows.Sources
{
    public class SqlDataSource<T> : IDataFlowSource<T> where T: new()
    {
        private readonly string _connectionString;
        private readonly string _sql;
        private SqlConnection _connection;
        private SqlDataReader _dataReader;
        
        public SqlDataSource(string connectionString, string sql)
        {
            _connectionString = connectionString;
            _sql = sql;
        }

        public DataFlowResult GetResults()
        {
            return new DataFlowResult();
        }

        public void Open()
        {
            if (_connection != null)
            {
                throw new Exception("Cannot open SqlDataSource connection twice");
            }
            _connection = new SqlConnection(this._connectionString);
            _connection.Open();
            var cmd = _connection.CreateCommand();
            cmd.CommandText = _sql;
            _dataReader = cmd.ExecuteReader();
        }
        

        public bool Read()
        {
            return _dataReader.Read();
        }

        public T GetItem()
        {
            var t = new T();
            var properties = typeof (T).GetProperties();
            foreach (var p in properties)
            {
                object value = _dataReader[p.Name];
                if (value == DBNull.Value)
                {
                    p.SetValue(t, null);
                }
                else
                {
                    p.SetValue(t, value);
                }
            }
            return t;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool diposing)
        {
            using (_dataReader) { }
            _dataReader = null;
            using (_connection) { }
            _connection = null;
        }
    }
}
