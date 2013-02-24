# SQL Data Flows .NET
DataFlows.NET is a replacement for SQL Server Integration Services (SSIS) designed 
to have a fluent programming model with proper intellisense, testability and 
extensibility. Much of what SSIS does is already built into .NET but leveraging the 
SSIS features often requires much more code that isn't easy to read. 

The goal of this library is to design code that is easy to read, easy to maintain 
and easy to test. 

## Test Driven Development
The library was built with a full suite of tests first to guide the overall design. 
You must have a local SQL Server instance running for the tests to work though.