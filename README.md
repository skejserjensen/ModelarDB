# ModelarDB
ModelarDB is a modular model-based time series management system that
interfaces a query engine and a storage system with ModelarDB Core. This Core
is a self-contained, adaptive, and highly extensible Java library for automatic
online compression of time series. ModelarDB is designed for Unix-like
operating systems and tested on Linux and macOS.

This repository primarily contains the static source code for the versions of
ModelalDB documented in the [papers listed
below](https://github.com/skejserjensen/ModelarDB#papers). The system is being
actively developed as an open-source project at
[ModelarDB](https://github.com/ModelarData/ModelarDB). While a re-design and
re-implementation in Rust is being actively developed as an open-source project
at [MiniModelarDB](https://github.com/ModelarData/MiniModelarDB).

## Installation
1. Install the Scala Build Tool (sbt).
2. Clone the ModelarDB Git repository https://github.com/skejserjensen/ModelarDB
3. Start ``sbt`` in the root of the repository and run one of the following commands:

- ``compile`` compiles ModelarDB to Java class files.
- ``package`` compiles ModelarDB to a Jar file.
- ``assembly`` compiles ModelarDB to an uber Jar file.*
- ``run`` executes ModelarDB's main method.**

\* To execute ModelarDB on an existing Apache Spark cluster, an uber jar must be
created to ensure the necessary dependencies are all included in a single jar.

\*\* If ``sbt run`` is executed directly from the command-line, then the run
command and the arguments must be surrounded by quotes to pass the arguments to
ModelarDB: ``sbt 'run arguments'``

## Configuration
ModelarDB requires that a configuration file is available at
*$HOME/Programs/modelardb.conf* or as the first command-line argument. This file
must specify the query processing engine and storage system to use. An example
configuration is included as part of this repository.

## Papers
The research leading to ModelarDB is documented in several papers. If you use
ModelarDB in academia, please cite the relevant paper(s) below.

***Scalable Model-Based Management of Correlated Dimensional Time Series in ModelarDB+***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *The Proceedings of ICDE, 1380-1391, 2021*\
Links: [IEEE](https://ieeexplore.ieee.org/document/9458830), [arXiv (preprint)](https://arxiv.org/abs/1903.10269)

***Model-Based Time Series Management at Scale***\
by Søren Kejser Jensen\
*PhD Thesis, The Technical Faculty of IT and Design, Aalborg University, 2019*\
Links: [AAU](https://vbn.aau.dk/en/publications/model-based-time-series-management-at-scale)

***Demonstration of ModelarDB: Model-Based Management of Dimensional Time Series***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *The Proceedings of SIGMOD, 1933-1936, 2019*\
Links: [ACM](https://dl.acm.org/doi/10.1145/3299869.3320216)

***ModelarDB: Modular Model-Based Time Series Management with Spark and Cassandra***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *The Proceedings of the VLDB Endowment, 11(11): 1688-1701, 2018*\
Links: [PVLDB](http://www.vldb.org/pvldb/vol11/p1688-jensen.pdf)

***Time Series Management Systems: A Survey***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *IEEE Transactions on Knowledge and Data Engineering, 29(11): 2581–2600, 2017*\
Links: [IEEE](https://ieeexplore.ieee.org/document/8012550/), [arXiv (preprint)](https://arxiv.org/abs/1710.01077)

## Presentations
The research leading to ModelarDB has also been presented at multiple events.
The slides used are available below:

***Model-based storage and management of massive sensor time series***\
by Christian Thomsen\
at *Digital Tech Summit, 2021*\
Links: [Slides](slides/2021-12-01_DTS.pdf), [Event](https://my.eventbuizz.com/event/digital-tech-summit-8890/detail)

***Extreme-Scale Model-Based Time Series Management with ModelarDB***\
by Torben Bach Pedersen as a Keynote Speaker\
at the *32nd International Conference on Database and Expert Systems Applications, 2021*\
Links: [Slides](slides/2021-09-30_DEXA-Keynote.pdf), [Event](http://www.dexa.org/previous/dexa2021/keynotes2021.html#keynote3)

***Extreme-Scale Model-Based Time Series Management with ModelarDB***\
by Torben Bach Pedersen as a Keynote Speaker\
at the *28th International Symposium on Temporal Representation and Reasoning, 2021*\
Links: [Slides](slides/2021-09-28_TIME-Keynote.pdf), [Abstract](https://drops.dagstuhl.de/opus/volltexte/2021/14778/), [Event](https://conference2.aau.at/event/61/page/46-time-2021)

***Extreme-Scale Model-Based Time Series Management with ModelarDB***\
by Torben Bach Pedersen as a Keynote Speaker\
at the *10th International Conference on Model and Data Engineering, 2021*\
Links: [Slides](slides/2021-06-21_MEDI-Keynote.pdf), [Event](https://cs.ttu.ee/events/medi2021/?page=keynotes)

***Model-Based Management of Correlated Dimensional Time Series***\
by Søren Kejser Jensen\
at *Dagstuhl Seminar 19282 (Data Series Management), 2019*\
Links: [Slides](slides/2019-07-08_Dagstuhl-Seminar-19282.pdf), [Rapport](https://drops.dagstuhl.de/opus/volltexte/2019/11634/), [Event](https://www.dagstuhl.de/en/program/calendar/semhp/?semnr=19282)

***Effektive metoder til at gemme og forespørge på store mængder tidsseriedata***\
by Christian Thomsen\
at *GrowAAL, 2019*\
Links: [Slides](slides/2019-05-07_GrowAAL.pdf), [Event](https://infinit.dk/big-data/anvend-data-til-at-sikre-din-investering/)

## License
ModelarDB is licensed under version 2.0 of the Apache License and a copy of the
license is bundled with the program.
