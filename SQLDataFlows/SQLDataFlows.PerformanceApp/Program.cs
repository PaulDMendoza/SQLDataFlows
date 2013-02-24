using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SQLDataFlows.Destinations;
using SQLDataFlows.Sources;
using SQLDataFlows.Tests;

namespace SQLDataFlows.PerformanceApp
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Created a lot of records and tests how long it takes to copy them.");
            var _connStr = FunctionalTests._connStr;

            Tests.FunctionalTests.ClassInit(null);
            using (var c = new SqlConnection(_connStr))
            {
                c.Open();

                var records = CreateSampleData(c);

                Console.WriteLine(DateTime.Now.ToLongTimeString() + " - Finished creating. Starting move.");

                var stw = new Stopwatch();
                stw.Start();

                Flow.OutOf(new SqlDataSource<TableSource>(_connStr, "select * from TableSource"))
                    .Into(new SqlDataDestination<TableDestination>(_connStr, "TableDestination"), mapping: source => new TableDestination() { Name = source.Name, State = source.State })
                    .Execute();

                stw.Stop();
                Console.WriteLine(DateTime.Now.ToLongTimeString() + " - Flow Time: " + stw.Elapsed);

                var cnt = c.ExecuteIntScalar("select count(0) from dbo.TableDestination");
                Assert.AreEqual(records, cnt, "Same number of records copied");
                Console.WriteLine("Push a key to continue...");
                Console.ReadLine();
            }
        }

        private static int CreateSampleData(SqlConnection c)
        {
            c.ExecuteStatement(@"CREATE TABLE [dbo].[TableSource](
	                        [Name] [varchar](50) NULL,
	                        [State] [varchar](50) NULL
                        ) ON [PRIMARY]");

            c.ExecuteStatement(@"CREATE TABLE [dbo].[TableDestination](
	                        [Name] [varchar](50) NULL,
	                        [State] [varchar](50) NULL
                        ) ON [PRIMARY]");
            Console.WriteLine(DateTime.Now.ToLongTimeString() + " - Creating records.");
            int records = 100000;
            for (int i = 0; i < records; i++)
            {
                c.ExecuteStatement("insert into TableSource VALUES ('" + Guid.NewGuid() + "', '" + Guid.NewGuid() + "');");
            }
            return records;
        }

        private class TableSource
        {
            public string Name { get; set; }
            public string State { get; set; }
        }

        private class TableDestination
        {
            public string Name { get; set; }
            public string State { get; set; }
        }
    }
}
