using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataFlows
{
    public class Flow
    {
        public static Flow<T> OutOf<T>(IDataFlowSource<T> source) where T : new()
        {
            var f = new Flow<T>();
            f.Source = source;
            return f;
        }
    }

    public class Flow<T> where T : new()
    {
        public Flow()
        {
            Destinations = new List<Destination>();
        }

        public IDataFlowSource<T> Source { get; set; }
        public IList<Destination> Destinations { get; set; }

        public class Destination
        {
            public IDataFlowDestination DataFlow { get; set; }
            public Func<T, object> Mapping { get; set; }
        }

        public void Execute()
        {
            ExecuteAsync().Wait();
        }

        public async Task ExecuteAsync()
        {
            await Task.Run(() =>
                {
                    using (Source)
                    {
                        Source.Open();
                        try
                        {
                            foreach (var d in Destinations)
                            {
                                d.DataFlow.Open();
                            }

                            while (Source.Read())
                            {
                                T item = Source.GetItem();
                                foreach (var d in Destinations)
                                {
                                    d.DataFlow.Write(d.Mapping(item));
                                }
                            }
                            foreach (var d in Destinations)
                            {
                                d.DataFlow.WritesCompleted();
                            }
                        }
                        finally
                        {
                            foreach (var d in Destinations)
                            {
                                using (d.DataFlow)
                                {
                                }
                            }
                        }
                    }
                });
        }
    }

    public static class FlowExtensions
    {
        public static Flow<T> Into<T, TOutput>(this Flow<T> flow, IDataFlowDestination<TOutput> destination, Func<T, TOutput> mapping)
            where T : new()
            where TOutput : new()
        {
            if (mapping == null)
            {
               throw new NullReferenceException("mapping parameter cannot be null"); 
            }

            flow.Destinations.Add(new Flow<T>.Destination()
            {
                DataFlow = destination,
                Mapping = source => mapping(source)
            });
            return flow;
        }
    }
}
