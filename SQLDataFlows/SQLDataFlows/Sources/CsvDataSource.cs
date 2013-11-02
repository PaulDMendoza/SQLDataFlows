using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;

namespace SQLDataFlows.Sources
{
    public class CsvDataSource<T> : IDataFlowSource<T> where T : new()
    {
        private CsvReader _reader;
        private StreamReader _sr;
        private string _fileName;

        private List<Action<CsvHelper.Configuration.CsvConfiguration>> _csvConfigActions = new List<Action<CsvConfiguration>>();

        public CsvDataSource(string fileName)
        {
            _fileName = fileName;
        }

        public void Dispose()
        {
            if (_sr != null)
                _sr.Close();
            if (_reader != null)
                _reader.Dispose();
        }

        public DataFlowResult GetResults()
        {
            return new DataFlowResult();
        }

        public void Open()
        {
            _sr = new StreamReader(_fileName);
            _reader = new CsvReader(_sr);
            foreach (var setConfigAction in _csvConfigActions)
            {
                setConfigAction(_reader.Configuration);
            }
        }

        public bool Read()
        {
            return _reader.Read();
        }

        public T GetItem()
        {
            return _reader.GetRecord<T>();
        }

        public CsvDataSource<T> Set(Action<CsvHelper.Configuration.CsvConfiguration> setConfig)
        {
            _csvConfigActions.Add(setConfig);
            return this;
        }
}
}
