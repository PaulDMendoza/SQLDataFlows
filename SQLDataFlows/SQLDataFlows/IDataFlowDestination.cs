using System;

namespace SQLDataFlows
{
    public interface IDataFlowDestination : IDisposable
    {
        void Open();
        void Write(dynamic item);
        void WritesCompleted();
    }

    public interface IDataFlowDestination<TOutput> :  IDataFlowDestination where TOutput : new()
    {
    }
}