using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using MyXPathReader;

namespace SQLDataFlows.Destinations
{
    public class XmlDataFlowSource<T> : IDataFlowSource<T> where T : new()
    {
        private String _xmlFileName;
        private string _xpath;
        public XmlDataFlowSource(string xmlFileName, string xpath)
        {
            _xmlFileName = xmlFileName;
            _xpath = xpath;
        } 

        public void Dispose()
        {
            
        }

        public DataFlowResult GetResults()
        {
            return new DataFlowResult();
        }

        private XPathReader _reader;
        public void Open()
        {
            _reader = new XPathReader(_xmlFileName, _xpath);
        }

        public bool Read()
        {
            while (_reader.ReadUntilMatch())
            {
                return true;
            }
            return false;
        }

        public T GetItem()
        {
            XmlSerializer serializer = new XmlSerializer(typeof(T));
            
            var outerXml = _reader.ReadOuterXml();
            using (TextReader reader = new StringReader(outerXml))
            {
                T result = (T)serializer.Deserialize(reader);
                return result;
            }
        }
    }
}
