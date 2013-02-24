using System.Collections.Concurrent;

namespace SQLDataFlows
{
    public class DataFlowResult
    {
        public IProducerConsumerCollection<object> QueueResult { get; set; }
        public bool IsComplete { get; set; }
    }
}