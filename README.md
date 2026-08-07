# ModelarDB
:warning: **This repository contains the static code used in papers; see [ModelarDB](https://github.com/ModelarData/ModelarDB-RS) for the open-source project.**

ModelarDB is an efficient high-performance time series management system that is designed to efficiently ingest, transfer, store, and analyze high-frequency time series across the edge, cloud, and client.
It provides state-of-the-art lossless compression, lossy compression, and query performance by efficiently compressing time series on the edge using multiple different types of models such as constant and linear functions.
As a result, the high-frequency time series can be transferred to the cloud through a connection with very limited bandwidth and stored in the cloud at a low cost.
The compressed time series can be efficiently queried on both the edge, cloud, and client using a relational interface and SQL without any knowledge about the model-based representation.
A query optimizer automatically rewrites the queries to exploit the model-based representation.
In addition, programming language bindings are provided so programs can efficiently execute queries against ModelarDB instances and read and write data on local disk or in a remote object store.

This repository contains the static source code and documentation for the versions of ModelarDB documented in the [papers listed below](https://github.com/skejserjensen/ModelarDB#papers).
Two versions of ModelarDB have been developed.
A version in Rust is currently being developed as an open-source project at [ModelarDB](https://github.com/ModelarData/ModelarDB-RS), and a legacy [JVM-based version of ModelarDB](https://github.com/ModelarData/ModelarDB).
Thus, the [Rust-based version of ModelarDB](https://github.com/ModelarData/ModelarDB-RS) is under active development, while the legacy [JVM-based version of ModelarDB](https://github.com/ModelarData/ModelarDB) is not.

## Papers
The research leading to ModelarDB is documented in several papers.
If you use ModelarDB in academia, please cite the relevant paper(s) below.

***Demonstration of ModelarDB: Model-Based Management of High-Frequency Time Series Across Edge, Cloud, and Client***\
by Søren Kejser Jensen, Christian Schmidt Godiksen, Christian Thomsen, Torben Bach Pedersen\
in *The Proceedings of the VLDB Endowment, 18(12): 5247-5250, 2025*\
Links: [PVLDB](https://www.vldb.org/pvldb/vol18/p5247-jensen.pdf), [GitHub](https://github.com/CGodiksen/modelardb-demo/tree/67f889428118318beb381e7f21a723a4c13c92ca)

***Scalable Model-Based Management of Massive High Frequency Wind Turbine Data with ModelarDB***\
by Abduvoris Abduvakhobov, Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *The Proceedings of the VLDB Endowment, 17(13): 4723-4732, 2024*\
Links: [PVLDB](https://www.vldb.org/pvldb/vol17/p4723-abduvakhobov.pdf), [AAU (extended)](https://vbn.aau.dk/ws/portalfiles/portal/771244882/v17_clean_revision_submitted_Extended_Version.pdf), [GitHub](https://github.com/aabduvakhobov/ModelarDB-Analyzer), [Slides](slides/2025-09-04_PVLDB.pdf)

***Why Model-Based Lossy Compression is Great for Wind Turbine Analytics***\
by Søren Kejser Jensen, Christian Thomsen, Torben Bach Pedersen, Carlos Enrique Muñiz-Cuza, and Abduvoris Abduvakhobov\
in *The Proceedings of ICDE, 5667-5668, 2024*\
Links: [IEEE](https://ieeexplore.ieee.org/document/10597779), [Slides](slides/2024-05-16_ICDE.pdf)

***Time Series Management Systems: A 2022 Survey***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *Data Series Management and Analytics (Forthcoming)*\
Links: [AAU (preprint)](https://vbn.aau.dk/da/publications/time-series-management-systems-a-2022-survey)

***ModelarDB: Integrated Model-Based Management of Time Series from Edge to Cloud***\
by Søren Kejser Jensen, Christian Thomsen, and Torben Bach Pedersen\
in *Transactions on Large-Scale Data- and Knowledge-Centered Systems LIII, 1-33, 2023*\
Links: [Springer](https://link.springer.com/chapter/10.1007/978-3-662-66863-4_1)

***Scalable Model-Based Management of Correlated Dimensional Time Series in ModelarDB+***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *The Proceedings of ICDE, 1380-1391, 2021*\
Links: [IEEE](https://ieeexplore.ieee.org/document/9458830), [arXiv (preprint)](https://arxiv.org/abs/1903.10269), [Slides](slides/2021-04-22_ICDE.pdf)

***Model-Based Time Series Management at Scale***\
by Søren Kejser Jensen\
*PhD Thesis, The Technical Faculty of IT and Design, Aalborg University, 2019*\
Links: [AAU](https://vbn.aau.dk/en/publications/model-based-time-series-management-at-scale), [Slides](slides/2019-11-04_PhD-Defence.pdf)

***Demonstration of ModelarDB: Model-Based Management of Dimensional Time Series***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *The Proceedings of SIGMOD, 1933-1936, 2019*\
Links: [ACM](https://dl.acm.org/doi/10.1145/3299869.3320216)

***ModelarDB: Modular Model-Based Time Series Management with Spark and Cassandra***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *The Proceedings of the VLDB Endowment, 11(11): 1688-1701, 2018*\
Links: [PVLDB](http://www.vldb.org/pvldb/vol11/p1688-jensen.pdf), [Slides](slides/2018-08-29_PVLDB.pdf)

***Time Series Management Systems: A Survey***\
by Søren Kejser Jensen, Torben Bach Pedersen, and Christian Thomsen\
in *IEEE Transactions on Knowledge and Data Engineering, 29(11): 2581–2600, 2017*\
Links: [IEEE](https://ieeexplore.ieee.org/document/8012550/), [arXiv (preprint)](https://arxiv.org/abs/1710.01077)

## Presentations
The research leading to ModelarDB has also been presented at multiple events.
The slides used are available below:

***ModelarDB: Analytics of High-Frequency Time Series Across Edge, Cloud, and Client***\
by Christian Thomsen and Søren Kejser Jensen\
at *Danish Digitalization, Data Science and AI, 2024*\
Links: [Event](https://d3aconference.dk/), [Slides](slides/2024-10-23_D3A.pdf)

***Model-based storage and management of massive sensor time series***\
by Christian Thomsen\
at *Digital Tech Summit, 2021*\
Links: [Event](https://my.eventbuizz.com/event/digital-tech-summit-8890/detail), [Slides](slides/2021-12-01_DTS.pdf)

***Extreme-Scale Model-Based Time Series Management with ModelarDB***\
by Torben Bach Pedersen as a Keynote Speaker\
at the *32nd International Conference on Database and Expert Systems Applications, 2021*\
Links: [Event](http://www.dexa.org/previous/dexa2021/keynotes2021.html#keynote3), [Slides](slides/2021-09-30_DEXA-Keynote.pdf)

***Extreme-Scale Model-Based Time Series Management with ModelarDB***\
by Torben Bach Pedersen as a Keynote Speaker\
at the *28th International Symposium on Temporal Representation and Reasoning, 2021*\
Links: [Event](https://conference2.aau.at/event/61/page/46-time-2021), [Abstract](https://drops.dagstuhl.de/opus/volltexte/2021/14778/), [Slides](slides/2021-09-28_TIME-Keynote.pdf)

***Extreme-Scale Model-Based Time Series Management with ModelarDB***\
by Torben Bach Pedersen as a Keynote Speaker\
at the *10th International Conference on Model and Data Engineering, 2021*\
Links: [Event](https://cs.ttu.ee/events/medi2021/?page=keynotes), [Slides](slides/2021-06-21_MEDI-Keynote.pdf)

***Model-Based Management of Correlated Dimensional Time Series***\
by Søren Kejser Jensen\
at *Dagstuhl Seminar 19282 (Data Series Management), 2019*\
Links: [Event](https://www.dagstuhl.de/en/program/calendar/semhp/?semnr=19282), [Rapport](https://drops.dagstuhl.de/opus/volltexte/2019/11634/), [Slides](slides/2019-07-08_Dagstuhl-Seminar-19282.pdf)

***Effektive metoder til at gemme og forespørge på store mængder tidsseriedata***\
by Christian Thomsen\
at *GrowAAL, 2019*\
Links: [Event](https://infinit.dk/big-data/anvend-data-til-at-sikre-din-investering/), [Slides](slides/2019-05-07_GrowAAL.pdf)

## License
ModelarDB is licensed under version 2.0 of the Apache License and a copy of the license is bundled with the program.