# ModelarDB
ModelarDB is a modular model-based time series management system that interfaces
a query engine and a storage system with ModelarDB Core. This Core is a
self-contained, adaptive and highly extensible Java library for automatic online
compression of time series. ModelarDB is designed for Unix-like operating
systems and tested on Linux and macOS.

We are always happy to hear from anybody who tries ModelarDB. From experience
reports we get a better understanding of the system's current limitations and
how to continue improving it. For any comment or requests regarding ModelarDB
please use GitHub issues so the discussion is kept public. For discussions that
cannot be public, you can, however, send us a private
[email](mailto:skj@cs.aau.dk "email").

## Installation
1. Install the Scala Build Tool *sbt*.
2. Clone the ModelarDB Git repository https://github.com/skejserjensen/ModelarDB
3. Start *sbt* in the root of the repository and run one of the following commands:

- **compile** compiles ModelarDB to Java class files.
- **package** compiles ModelarDB to a Jar file.
- **assembly** compiles ModelarDB to an uber Jar file.*
- **run** executes ModelarDB's main method.

\* To execute ModelarDB on an existing Apache Spark cluster, an uber jar must
be created; use [sbt-assembly](https://github.com/sbt/sbt-assembly) to ensure
the necessary dependencies are included as part of this archive.

## Papers
The work leading to ModelarDB is documented in a number of papers. If you use
ModelarDB in academia, please cite the relevant paper(s) below.

***Time Series Management Systems: A Survey***  
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen  
in *IEEE Transactions on Knowledge and Data Engineering, 29(11): 2581–2600, 2017*  
Download: [IEEE](https://ieeexplore.ieee.org/document/8012550/), [arXiv](https://arxiv.org/abs/1710.01077)

***ModelarDB: Modular Model-Based Time Series Management with Spark and Cassandra***  
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen  
in *The Proceedings of the VLDB Endowment, 11(11): 1688-1701, 2018*  
Download: [PVLDB](http://www.vldb.org/pvldb/vol11/p1688-jensen.pdf)

***Demonstration of ModelarDB: Model-Based Management of Dimensional Time Series***  
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen  
in *The Proceedings of SIGMOD, 1933-1936, 2019*  
Download: [ACM](https://dl.acm.org/doi/10.1145/3299869.3320216)

***Model-Based Time Series Management at Scale***  
by Søren Kejser Jensen  
*PhD Thesis, The Technical Faculty of IT and Design, Aalborg University, 2019*  
Download: [AAU](https://vbn.aau.dk/en/publications/model-based-time-series-management-at-scale)

***Scalable Model-Based Management of Correlated Dimensional Time Series in ModelarDB+***  
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen  
to appear in *The Proceedings of ICDE, 2021*  
Download: [arXiv](https://arxiv.org/abs/1903.10269)

NOTE: The papers use a Tid (Time series identifier) to uniquely identify time
series while the open-source implementation of ModelarDB uses a Sid (Source
identifier) due to a naming conflict in the initial implementation.

## Configuration
ModelarDB requires that a configuration file is available at
*$HOME/Programs/modelardb.conf* or as the first command-line argument. This file
must specify the query processing engine and storage system to use. An example
configuration is included as part of this repository.

## License
ModelarDB is licensed under version 2.0 of the Apache License and a copy of the
license is bundled with the program.
