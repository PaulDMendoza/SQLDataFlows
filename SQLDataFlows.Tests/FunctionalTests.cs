using System;
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
        public void MoveData()
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


                c.ExecuteStatement("insert into TableSource VALUES ('Paul', 'CA');")
                    .ExecuteStatement("insert into TableSource VALUES ('John', 'CA');")
                    .ExecuteStatement("insert into TableSource VALUES ('James', 'CA');")
                    .ExecuteStatement("insert into TableSource VALUES ('Mark', 'NV');")
                    .ExecuteStatement("insert into TableSource VALUES ('Webb', NULL);");

                var stw = new Stopwatch();
                stw.Start();

                Flow.OutOf(new SqlDataSource<TableSource>(_connStr, "select * from TableSource"))
                    .Into(new SqlDataDestination<TableDestination>(_connStr, "TableDestination"), mapping: source=> new TableDestination() { Name = source.Name, State = source.State })
                    .Execute();

                stw.Stop();
                Console.WriteLine("Flow Time: " + stw.Elapsed);

                var cnt = c.ExecuteIntScalar("select count(0) from dbo.TableDestination");
                Assert.AreEqual(5, cnt);
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
    }
}
