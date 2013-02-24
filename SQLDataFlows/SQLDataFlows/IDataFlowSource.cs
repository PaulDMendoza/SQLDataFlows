using System;

namespace SQLDataFlows
{
    public interface IDataFlowSource<T>: IDisposable where T : new()
    {
        DataFlowResult GetResults();
        void Open();
        bool Read();
        T GetItem();
    }
}