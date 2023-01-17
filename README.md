# Slon
_Slon (Slovene: Elephant)_

A modern high performance PostgreSQL protocol implementation for .NET

Slon is:
- A low-level driver with a strictly layered ADO.NET implementation.
- Concurrent and pipelineable by default.
- Built on top of System.IO.Pipelines.
- Low/zero steady state allocation overhead.
- A playground for ADO.NET experiments.


Special Thanks:
This project has drawn code, inspiration and lessons from the fantastic Npgsql project https://github.com/npgsql/npgsql
Additionally some of the code around pipelines is based on implementations from https://github.com/dotnet/runtime