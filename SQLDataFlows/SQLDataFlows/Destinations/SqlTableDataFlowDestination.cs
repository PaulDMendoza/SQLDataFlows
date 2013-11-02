using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataFlows.Destinations
{
    public class SqlTableDataFlowDestination <TOutput>: IDataFlowDestination<TOutput> where TOutput : new()
    {
        private readonly string _connectionString;
        private readonly string _tableName;
        private SqlConnection _connection;

        private DataTable _dataTable;

        private PropertyInfo[] _properties; 

        public SqlTableDataFlowDestination(string connectionString, string tableName)
        {
            _connectionString = connectionString;
            _tableName = tableName;
        }

        public void Open()
        {
            if (_connection != null)
            {
                throw new Exception("Cannot open SqlTableDataFlowSource connection twice");
            }
            _connection = new SqlConnection(this._connectionString);
            _connection.Open();
        }

        public void Write(dynamic item)
        {
            if (_dataTable == null)
            {
                _dataTable = new DataTable(_tableName);

                if (_properties == null)
                {
                    _properties = item.GetType().GetProperties();
                }

                foreach (var p in _properties)
                {
                    _dataTable.Columns.Add(p.Name);
                }
            }
            var row = _dataTable.NewRow();
            foreach (PropertyInfo p in item.GetType().GetProperties())
            {
                row[p.Name] = p.GetValue(item);
            }
            _dataTable.Rows.Add(row);

            if (_dataTable.Rows.Count > 500)
            {
                BulkCopy();
            }
        }

        public void WritesCompleted()
        {
            BulkCopy();
        }

        private void BulkCopy()
        {
            if (_properties == null)
            {
                return;
            }
            var bulkCopy = new SqlBulkCopy(_connection);
            bulkCopy.DestinationTableName = _tableName;
            foreach (PropertyInfo p in _properties)
            {
                bulkCopy.ColumnMappings.Add(p.Name, p.Name);
            }
            bulkCopy.WriteToServer(_dataTable);
            _dataTable = null;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool diposing)
        {
            using (_connection) { }
            _connection = null;
            using(_dataTable) {}
            _dataTable = null;
        }
    }
}
