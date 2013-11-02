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
    public class MemoryDataFlowDestination <TOutput>: IDataFlowDestination<TOutput> where TOutput : new()
    {
        public List<TOutput> Output { get; set; } 
        
        public MemoryDataFlowDestination(List<TOutput> outputCollection )
        {
            Output = outputCollection;
        }

        public void Open()
        {
            
        }

        public void Write(dynamic item)
        {
            if(item is TOutput)
                Output.Add(item);
        }

        public void WritesCompleted()
        {
            
        }

        private void BulkCopy()
        {
            
        }

        public void Dispose()
        {
            
        }

        protected virtual void Dispose(bool diposing)
        {
            
        }
    }
}
