# ModelarDB
ModelarDB is a modular model-based time series management system that interfaces
a query engine and a storage system with ModelarDB Core. This Core is a
self-contained, adaptive and highly extensible Java library for automatic online
compression of time series. ModelarDB is designed for Unix-like operating
systems and tested on Linux and macOS.

## Installation
1. Install the Scala Build Tool *sbt*.
2. Start *sbt* in the root of this repository and run one of the following commands:

- **compile** compiles ModelarDB to Java class files.
- **package** compiles ModelarDB to a Jar file.
- **assembly** compiles ModelarDB to an Uber Jar file.
- **run** executes ModelarDB's main method.

NOTE: To execute ModelarDB on an existing Apache Spark cluster an uber jar must
be created; use [sbt-assembly](https://github.com/sbt/sbt-assembly) to ensure
the necessary dependencies are included as part of this archive.

## Papers
***Time Series Management Systems: A Survey***  
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen  
in *IEEE Transactions on Knowledge and Data Engineering, 29(11):2581–2600, 2017*  
Download: [IEEE Xplore Digital Library](https://ieeexplore.ieee.org/document/8012550/), [arXiv](https://arxiv.org/abs/1710.01077)

***ModelarDB: Modular Model-Based Time Series Management with Spark and Cassandra***  
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen  
in *The Proceedings of the VLDB Endowment, 11(11): 1688-1701, 2018*  
Download: [PVLDB](http://www.vldb.org/pvldb/vol11/p1688-jensen.pdf)

NOTE: The PVLDB paper from 2018 uses a Tid (Time series identifier) to uniquely
identify time series while the corresponding version of ModelarDB uses a Sid
(Source identifier) due to a naming conflict in the implementation.

## Configuration
ModelarDB requires that a configuration file is available at
*$HOME/Programs/modelardb.conf* or as the first command line parameter. This
file must specify the query processing engine and storage system to use. An
example configuration is included as part of this repository.

## License
ModelarDB is licensed under version 2.0 of the Apache License and a copy of the
license is bundled with the program.
