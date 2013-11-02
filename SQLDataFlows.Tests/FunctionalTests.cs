using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Practices.Unity;
using Moq;
using SQLDataFlows.Destinations;
using SQLDataFlows.Sources;
using System.Diagnostics;

namespace SQLDataFlows.Tests
{
    [TestClass]
    public class FunctionalTests
    {
        /// <summary>
        /// Change this if you want to test locally to whatever your SQL Server is named. 
        /// The database will be auto generated.
        /// </summary>
        public const string _connStr = "Data Source=.\\SQLSERVER;Initial Catalog=SQLDataFlowsTests;Integrated Security=true;";

        [ClassInitialize]
        public static void ClassInit(TestContext context)
        {
            var masterConnection = new SqlConnectionStringBuilder(_connStr);
            masterConnection.InitialCatalog = "master";
            using (var c = new SqlConnection(masterConnection.ToString()))
            {
                c.Open();
                var cnt = c.ExecuteIntScalar("select count(0) from sys.databases where name = 'SQLDataFlowsTests'");
                if (cnt > 0)
                {
                    c.ExecuteStatement("alter database [SQLDataFlowsTests] set single_user with rollback immediate;");
                    c.ExecuteStatement("DROP DATABASE [SQLDataFlowsTests]");
                }
                c.ExecuteStatement("CREATE DATABASE [SQLDataFlowsTests];");


            }
        }

        [TestMethod]
        public void MoveDataToMultipleDestinations()
        {
            using (var c = new SqlConnection(_connStr))
            {
                c.Open();

                c.ExecuteStatement(@"CREATE TABLE [dbo].[TableSource](
	                        [Name] [varchar](50) NULL,
	                        [State] [varchar](50) NULL
                        ) ON [PRIMARY]");

                c.ExecuteStatement(@"CREATE TABLE [dbo].[TableDestination](
	                        [Name] [varchar](50) NULL,
	                        [State] [varchar](50) NULL
                        ) ON [PRIMARY]");
                c.ExecuteStatement(@"CREATE TABLE [dbo].[TableDestination2](
	                        [Name] [varchar](50) NULL,
	                        [State] [varchar](50) NULL
                        ) ON [PRIMARY]");


                c.ExecuteStatement("insert into TableSource VALUES ('Paul', 'CA');")
                 .ExecuteStatement("insert into TableSource VALUES ('John', 'CA');")
                 .ExecuteStatement("insert into TableSource VALUES ('James', 'CA');")
                 .ExecuteStatement("insert into TableSource VALUES ('Mark', 'NV');")
                 .ExecuteStatement("insert into TableSource VALUES ('Webb', NULL);")
                 .ExecuteStatement("insert into TableSource VALUES ('Keith', 'MS');")
                 .ExecuteStatement("insert into TableSource VALUES ('Todd', 'WS');")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource")
                 .ExecuteStatement("insert into TableSource select * from TableSource");



                var stw = new Stopwatch();
                stw.Start();

                Flow.OutOf(new SqlTableDataFlowSource<TableSource>(_connStr, "select * from TableSource"))
                    .Into(new SqlTableDataFlowDestination<TableDestination>(_connStr, "TableDestination2"),
                            mapping: source =>
                                {
                                    var td = new TableDestination()
                                        {
                                            Name = source.Name.ToLower(),
                                            State = source.State
                                        };
                                    if (td.Name.StartsWith("D"))
                                        td.Name = "Not a valid name";

                                    return td;
                                })
                    .Into(new SqlTableDataFlowDestination<TableDestination>(_connStr, "TableDestination"),
                            mapping: source => new TableDestination()
                            {
                                Name = source.Name.ToUpper(),
                                State = source.State
                            })
                    .Execute();

                stw.Stop();
                Console.WriteLine("Flow Time: " + stw.Elapsed);



                var cnt1 = c.ExecuteIntScalar("select count(0) from dbo.TableDestination");
                Assert.AreNotEqual(0, cnt1);

                var cnt2 = c.ExecuteIntScalar("select count(0) from dbo.TableDestination2");
                Assert.AreNotEqual(0, cnt2);

                Console.WriteLine("Total Rows: " + (cnt1 + cnt2).ToString());
            }
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

        [TestMethod]
        public void TestCSV()
        {
            var stw = new Stopwatch();
            stw.Start();

            List<FakeData1> fakeDatas = new List<FakeData1>();

            Flow.OutOf(
                new CsvDataSource<FakeData1>("TestFiles\\fakedata1.csv")
                    .Set(s=>s.AllowComments = true)
                    .Set(s=>s.Delimiter = ",")
                    .Set(s=>s.IgnoreHeaderWhiteSpace = true))
                .Into(new MemoryDataFlowDestination<FakeData1>(fakeDatas), mapping: a => a)
                .Execute();

            stw.Stop();
            Console.WriteLine("Flow Time: " + stw.Elapsed);
            Assert.AreNotEqual(0, fakeDatas.Count);
        }

        public class FakeData1
        {
            public string Name { get; set;  }
            public int Age { get; set; }
            public string City { get; set; }
        }


        [TestMethod]
        public void TestXML()
        {
            var stw = new Stopwatch();
            stw.Start();

            List<food> fakeDatas = new List<food>();

            Flow.OutOf(new XmlDataFlowSource<food>("TestFiles\\fakexml.xml", @"//breakfast_menu/food"))
                .Into(new MemoryDataFlowDestination<food>(fakeDatas), mapping: a => a)
                .Execute();

            stw.Stop();
            Console.WriteLine("Flow Time: " + stw.Elapsed);
            Assert.AreEqual(5, fakeDatas.Count);
        }

        public class food
        {
            public string name { get; set; }
            public string price { get; set; }
            public string description { get; set; }
            public int calories { get; set; }

        }

    }
}
