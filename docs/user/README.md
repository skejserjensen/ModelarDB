# ModelarDB Installation and Usage
This document describes how to set up and use ModelarDB. Installation instructions are provided for Linux, macOS,
FreeBSD, and Windows. To support running ModelarDB in a containerized environment, instructions for setting up a Docker
environment are also provided. Once installed, using ModelarDB is consistent across all platforms.

## Installation
### Linux
The following commands are for Ubuntu Server. However, equivalent commands should work for other Linux distributions.

1. Install [build-essential](https://packages.ubuntu.com/jammy/build-essential): `sudo apt install build-essential`

### macOS
1. Install the Xcode Command Line Developer Tools: `xcode-select --install`

### FreeBSD
1. Install [cURL](https://curl.se/) as the *root* user: `pkg install curl`

### Windows
1. Install the latest versions of the Microsoft Visual C++ Prerequisites for Rust:
   - Microsoft Visual C++ Prerequisites for Rust: see [The rustup
   book](https://rust-lang.github.io/rustup/installation/windows-msvc.html).

### All
2. Install the latest stable [Rust Toolchain](https://rustup.rs/).
3. Install the [Protocol Buffer Compiler](https://protobuf.dev/installation/).
4. Clone the repository: `git clone https://github.com/ModelarData/ModelarDB-RS`
5. Build, test, and run the system using Cargo:
   - Debug Build: `cargo build`
   - Release Build: `cargo build --release`
   - Run Tests: `cargo test`
   - Run DBMS Server: `cargo run --bin modelardbd path_to_local_data_folder`
   - Run Client: `cargo run --bin modelardb [server_address] [query_file]`
6. Move `modelardbd`, `modelardbm`,`modelardb`, and `modelardbb` from the `target` directory to any
   directory.
7. Install and test the Python bindings for `modelardb_embedded` using Python:
   * Install: `python3 -m pip install .`
   * Run Tests: `python3 -m unittest`

## Usage
ModelarDB consists of four binaries and a library with bindings: `modelardbd` is a DBMS server that manages data and
executes SQL queries, `modelardbm` is a manager for one or more `modelardbd` instances deployed on the *edge* or in the
*cloud*, `modelardb` is a command-line client for connecting to a `modelardbd` instance and executing commands and SQL
queries, `modelardbb` is a command-line bulk loader that operates without `modelardbd` as it reads from and writes to
`modelardbd`'s data folder directly, and `modelardb_embedded` is an embeddable library for executing queries against and
writing to `modelardbd` or its data folder directly. `modelardbd` uses local storage on the edge and an Amazon
S3-compatible or Azure Blob Storage object store in the cloud. `modelardbd` can be deployed alone or together with
`modelardbm` depending on the use cases.

`modelardbd` can be deployed on a single node to manage data in a local folder. In this configuration `modelardbm` is
not needed. `modelardbd` can also be deployed in a distributed configuration across edge and cloud. In this
configuration, `modelardbm` must first be deployed in the cloud to manage the `modelardbd` instances on the edge and in
the cloud. Specifically, `modelardbm` is responsible for keeping the database schema consistent across all `modelardbd`
instances in the cluster. After deploying `modelardbm`, instances of `modelardbd` can be deployed on the edge and in
cloud. While the `modelardbd` instances on the edge and in cloud provides the same functionality, the primary purpose of
the instances on the edge is to collect data and transfer it to an object store in the cloud while the primary purpose
of the instances in the cloud is to execute queries on the data in the object store.

Thus, when `modelardbd` is deployed in edge mode, it executes queries against local storage for low latency queries on
the latest data and when it is deployed in cloud mode, it executes queries against the object store for efficient
analytics on all the data transferred to the cloud. `modelardbm` implements the [Apache Arrow Flight
protocol](https://arrow.apache.org/docs/format/Flight.html#downloading-data) for looking up the `modelardbd` instance in
the cloud to use for executing each query, thus providing a single, workload-balanced, interface for querying the data
in the object store using the `modelardbd` instances in the cloud.

### Start Server
There are three options available when starting `modelardbd` depending on the desired deployment use case. Each option
has different requirements and supports different features.

1. Start `modelardbd` in edge mode using only local storage - This is a simple and easy-to-use single-node deployment
for use cases where a redundant object store is not required, e.g., when testing or experimenting. In edge mode without
an object store, data is ingested to and queried from local storage. Thus, `modelardbd` instances in this configuration
do not support transferring ingested data to an object store and do not support querying data from the object store. To
run `modelardbd` in edge mode using only local storage, simply pass the path to the local folder `modelardbd` should use
as its data folder:

```shell
modelardbd path_to_local_data_folder
# or
modelardbd edge path_to_local_data_folder
```

2. Start `modelardbd` in edge mode with a manager - A running instance of `modelardbm` and an Amazon S3-compatible or
Azure Blob Storage object store are required to start `modelardbd` in edge mode with a manager. This configuration
supports ingesting data to a local folder on the edge, querying the data in the local folder on the edge, and
transferring data in the local data folder to the object store.
3. Start `modelardbd` in cloud mode with a manager - As above, a running instance of `modelardbm` and an Amazon
S3-compatible or Azure Blob Storage object store are required to start `modelardbd` in cloud mode with a manager. This
configuration supports ingesting data to a local folder in the cloud, transferring data in the local data folder to the
object store, and querying the data in the object store in the cloud.

The following environment variables must be set to appropriate values if an Amazon S3-compatible object store is used so
`modelardbm` and `modelardbd` know how to connect to it:

```shell
AWS_ENDPOINT
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

For example, to use a local instance of [MinIO](https://min.io/), assuming the access key id `KURo1eQeMhDeVsrz` and the
secret access key `sp7TDyD2ZruJ0VdFHmkacT1Y90PVPF7p` have been created through MinIO's command line tool or web
interface, set the environment variables as follows:

```shell
AWS_ENDPOINT="http://127.0.0.1:9000"
AWS_ACCESS_KEY_ID="KURo1eQeMhDeVsrz"
AWS_SECRET_ACCESS_KEY="sp7TDyD2ZruJ0VdFHmkacT1Y90PVPF7p"
```

Then, assuming a bucket named `wind-turbine` has been created through MinIO's command line tool or web interface,
`modelardbm` can be started with the following command:

```shell
modelardbm s3://wind-turbine
```

`modelardbm` also supports using [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/)
for the remote object store. To use Azure Blob Storage, set the following environment variables:

```shell
AZURE_STORAGE_ACCOUNT_NAME
AZURE_STORAGE_ACCESS_KEY
```

Then, assuming a container named `wind-turbine` has been created, `modelardbm` can be started with the following
command:

```shell
modelardbm azureblobstorage://wind-turbine
```

When a manager is running, `modelardbd` can be started in edge mode with transfer of the ingested data to the object
store given to `modelardbm` using the following command:

```shell
modelardbd edge path_to_local_data_folder manager_url
````

To run `modelardbd` in cloud mode simply replace `edge` with `cloud` as shown below. Be aware that when running in cloud
mode the `modelardbd` instance will execute queries against the object store given to `modelardbm` and not against local
storage.

```shell
modelardbd cloud path_to_local_data_folder manager_url
```

In both cases, access to the object store is automatically provided by the manager when `modelardbd` instances are
connected to it. Note that the manager uses `9998` as the default port and that the DBMS server uses `9999` as the
default port. The ports can be changed by specifying different ports with the following environment variables:

```shell
MODELARDBM_PORT=8888  # By default modelardbm uses port 9998.
MODELARDBD_PORT=8889  # By default modelardbd uses port 9999.
```

### Ingest Data
Before data can be ingested into `modelardbd`, tables must be created. `modelardbd` supports two types of tables,
standard relational tables created with `CREATE TABLE` statements and time series tables created with `CREATE TIME SERIES TABLE`
statements. From a user's perspective, a time series table functions like a standard relational table and can be queried 
using SQL. However, the implementation of a time series table is highly optimized for time series and a time series table 
must contain a single column with timestamps, one or more columns with fields (measurements as floating-point values), 
and zero or more columns with tags (metadata as strings). As stated, time series tables can be created using 
`CREATE TIME SERIES TABLE` statements with the column types `TIMESTAMP`, `FIELD`, and `TAG`. For `FIELD`, an error bound 
can optionally be specified in parentheses to enable lossy compression with a per-value error bound. The error bound can 
be absolute or relative, e.g.,`FIELD(1.0)` creates a column with an absolute per-value error bound that allows each value 
to deviate by at most 1.0 while `FIELD(1.0%)` creates a column with a relative per-value error bound that allows each 
value to deviate by at most 1.0%. `FIELD` columns default to an error bound of zero when none is specified. Thus, by 
default lossless compression is used and lossy compression is only used if explicitly requested. If the values in a 
`FIELD` column can be computed from other columns they need not be stored. Instead, if a `FIELD` column is defined using 
the syntax `FIELD AS (expression)`, e.g., `FIELD AS (column_one + column_two)`, the values of the `FIELD` column will be 
the result of the expression. As these generated `FIELD` columns do not store any data, an error bound cannot be defined.

As both `CREATE TIME SERIES TABLE` and `CREATE TABLE` are just SQL statements, both types of tables can be created using
`modelardb` or programmatically using Apache Arrow Flight. For example, a time series table storing a simple multivariate
time series with weather data collected at different wind turbines can be created as follows:

```shell
CREATE TIME SERIES TABLE wind_turbine(timestamp TIMESTAMP, wind_turbine TAG, wind_direction FIELD, wind_speed FIELD(1.0%))
```

The following example shows how to create the same time series table in Python using Apache Arrow Flight:

```python
from pyarrow import flight

flight_client = flight.FlightClient("grpc://127.0.0.1:9999")

sql = "CREATE TIME SERIES TABLE wind_turbine(timestamp TIMESTAMP, wind_turbine TAG, wind_direction FIELD, wind_speed FIELD(1.0%))"
ticket = flight.Ticket(sql)
result = flight_client.do_get(ticket)

print(list(result))
```

When running a cluster of `modelardbd` instances, it is required to use `modelardbm` to create tables. This is a 
necessity as `modelardbm` is responsible for keeping the database schema consistent across all `modelardbd` instances in 
the cluster. The process for creating a table on the manager is the same as when creating the table directly on a 
`modelardbd` instance, as shown above. The only difference is that the gRPC URL should be changed to connect to the 
manager instead of the DBMS server. When the table is created through `modelardbm`, the table is automatically created 
in all `modelardbd` instances managed by `modelardbm`.

After creating a table, data can be ingested into `modelardbd` with `INSERT` in `modelardb`. Be aware that `INSERT` 
statements currently must contain values for all columns but that the values for generated columns will be dropped by 
`modelardbd`. As parsing `INSERT` statements add significant overhead, binary data can also be ingested programmatically
using Apache Arrow Flight. For example, this Python example ingests three data points into the time series table 
`wind_turbine`:

```python
import pyarrow
from pyarrow import flight

# Read the data into a PyArrow table.
timestamp = pyarrow.array([100, 200, 300])
wind_turbine = pyarrow.array(["1026", "1026", "1026"])
wind_direction = pyarrow.array([300.0, 300.0, 300.0])
wind_speed = pyarrow.array([4.0, 4.0, 4.0])
names = ["timestamp", "wind_turbine", "wind_direction", "wind_speed"]
table = pyarrow.Table.from_arrays([timestamp, wind_turbine, wind_direction, wind_speed], names=names)

# Push the table to modelardbd's Apache Arrow Flight do_put() endpoint.
flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
upload_descriptor = flight.FlightDescriptor.for_path("wind_turbine")
writer, _ = flight_client.do_put(upload_descriptor, table.schema)
writer.write(table)
writer.close()
```

While this example simply ingests three data points from memory, it is simple to extend such that it reads from other
data sources. For example, [this Python
script](https://github.com/ModelarData/Utilities/blob/main/Apache-Parquet-Loader/main.py) makes it simple to bulk load
time series from Apache Parquet files with the same schema by reading the Apache Parquet files, creating a time series 
table that matches their schema if it does not exist, and transferring the data in the Apache Parquet files to `modelardbd`
using Apache Arrow Flight.

Time series can also be ingested into `modelardbd` using
[Telegraf](https://www.influxdata.com/time-series-platform/telegraf/) with the [Apache Arrow Flight output
plugin](https://github.com/ModelarData/Telegraf-Output-Apache-Arrow-Flight). By using Telegraf, data points can be
efficiently streamed into `modelardbd` from a large [collection of data
sources](https://www.influxdata.com/time-series-platform/telegraf/telegraf-input-plugin/) such as
[MQTT](https://mqtt.org/) and [OPC-UA](https://opcfoundation.org/about/opc-technologies/opc-ua/).

It should be noted that the ingested data is only transferred to the remote object store when `modelardbd` is deployed
in edge mode with a manager or in cloud mode with a manager. When `modelardbd` is deployed in edge mode without a
manager, the ingested data is only stored in local storage.

### Execute Queries
ModelarDB includes a command-line client in the form of `modelardb`. To interactively execute SQL statements against a
local instance of `modelardbd` through a REPL, simply run `modelardb`:

```shell
modelardb
```

Be aware that the REPL currently does not support splitting SQL statements over multiple lines, thus SQL statements do
not need to end with a `;`. In addition to `CREATE TIME SERIES TABLE`, ModelarDB also extends SQL with an `INCLUDE` clause
with the format `INCLUDE address[, address]*`. When this clause is prepended to a `SELECT` statement, a `modelardbd`
instance executes the `SELECT` statement on the data it manages and forwards the statement to `modelardbd` instances at
the provided addresses. Afterwards, the `modelardbd` instance that initially received the query, unions the result from
all `modelardbd` instances and returns it to the client. As an example, this can be used to execute a `SELECT` statement
on both the data in the cloud and a specific edge node. Although, be aware that this does not make the edge node
transfer the data it manages to the cloud, only the result of the query. In addition to SQL statements, the REPL also
supports listing all tables using `\dt`, printing the schema of a table using `\d table_name`, flushing data in memory
to disk using `\f`, flushing data in memory and on disk to an object store using `\F`, and printing operations supported
by the client using `\h`.

```sql
ModelarDB> \dt
wind_turbine
ModelarDB> \d wind_turbine
timestamp: Timestamp(Microsecond, None)
wind_turbine: Utf8
wind_direction: Float32
wind_speed: Float32, Error Bound 1%
ModelarDB> SELECT * FROM wind_turbine LIMIT 5
+-------------------------+--------------+------------+----------------+
| timestamp               | wind_turbine | wind_speed | wind_direction |
+-------------------------+--------------+------------+----------------+
| 2020-01-01T00:00:00.000 | KL3773       | 4.7987617  | 12.195049      |
| 2020-01-01T00:00:00.100 | KL3773       | 4.7562086  | 10.183235      |
| 2020-01-01T00:00:00.200 | KL3773       | 4.7384476  | 9.4519857      |
| 2020-01-01T00:00:00.300 | KL3773       | 4.7108905  | 9.8607325      |
| 2020-01-01T00:00:00.400 | KL3773       | 4.6879335  | 10.083183      |
+-------------------------+--------------+------------+----------------+
ModelarDB>
```

If `modelardbd` is not running on the same host, the host `modelardb` should connect to must be specified.
`modelardbd`'s Apache Arrow Flight interface accepts requests on port `9999` by default, so it is generally not
necessary to specify a port:

```shell
modelardb 10.0.0.37
```

Also, if `modelardbd` has been configured to use another port using the environment variable `MODELARDBD_PORT`, the same
port must also be passed to the client:

```shell
modelardb 10.0.0.37:9998
```

`modelardb` can also execute SQL statements from a file passed as a command-line argument:

```shell
modelardb 10.0.0.37 path_to_file_with_sql_statements.sql
```

`modelardbd` can also be queried programmatically [from many different programming
languages](https://arrow.apache.org/docs/index.html) using Apache Arrow Flight. The following Python example shows how
to execute a simple SQL query against `modelardbd` and process the resulting stream of data points using
[`pyarrow`](https://pypi.org/project/pyarrow/) and [`pandas`](https://pypi.org/project/pandas/). A PEP 249 compatible
connector is also available for Python in the form of [PyModelarDB](https://github.com/ModelarData/PyModelarDB).

```python
from pyarrow import flight

flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
ticket = flight.Ticket("SELECT * FROM wind_turbine LIMIT 10")
flight_stream_reader = flight_client.do_get(ticket)

for flight_stream_chunk in flight_stream_reader:
    record_batch = flight_stream_chunk.data
    pandas_data_frame = record_batch.to_pandas()
    print(pandas_data_frame)
```

When running a cluster of `modelardbd` instances, it is recommended to use `modelardbm` to query the data in the remote
object store. As mentioned above, `modelardbm` implements the [Apache Arrow Flight
protocol](https://arrow.apache.org/docs/format/Flight.html#downloading-data) for querying data. This means that a
request is made to the manager first to determine which `modelardbd` cloud instance in the cluster should be queried.
The workload of the queries sent to the manager is balanced across all `modelardbd` cloud instances and at least one
`modelardbd` cloud instance must be running for the manager to accept queries. The following Python example shows how to
execute a simple SQL query using a `modelardbm` instance and how to process the resulting stream of data points using
[`pyarrow`](https://pypi.org/project/pyarrow/) and [`pandas`](https://pypi.org/project/pandas/). It should be noted that
the manager is only responsible for workload balancing and that the query is sent directly to the `modelardbd` cloud
instance chosen by the manager which then sends the result set directly back to the client.

```python
from pyarrow import flight

manager_client = flight.FlightClient("grpc://127.0.0.1:9998")
query_descriptor = flight.FlightDescriptor.for_command("SELECT * FROM wind_turbine LIMIT 10")
flight_info = manager_client.get_flight_info(query_descriptor)

endpoint = flight_info.endpoints[0]
cloud_node_url = endpoint.locations[0]

cloud_client = flight.FlightClient(cloud_node_url)
flight_stream_reader = cloud_client.do_get(endpoint.ticket)

for flight_stream_chunk in flight_stream_reader:
    record_batch = flight_stream_chunk.data
    pandas_data_frame = record_batch.to_pandas()
    print(pandas_data_frame)
```

### Embed Library
ModelarDB includes an embeddable library in the form of `modelardb_embedded`. It allows programming languages to execute
queries against or write to `modelardbd`, `modelardbm`, or a data folder directly. A C-API allows other programming
languages than Rust to also use `modelardb_embedded`. The location where queries and writes are executed is specified
using the set of `open_*()` methods and the `connect()` method. The other methods in `modelardb_embedded` work for both
local disk, object store, `modelardbd`, and `modelardbm` with a few exceptions. Thus, a program can be developed against
a small data set in a local data folder and then scaled by switching to `modelardbd`.

```python
import modelardb

# Execute queries and write to a data folder on a local disk.
local_data_folder = modelardb.open_local(data_folder_path)

# Execute queries and write to a data folder in a S3-compatible object store.
remote_data_folder = modelardb.open_s3(endpoint, bucket_name, access_key_id, secret_access_key)

# Execute queries and write to a data folder in Microsoft Azure Blob Storage.
remote_data_folder = modelardb.open_azure(account_name, access_key, container_name)

# Execute queries and write to a `modelardbd` or `modelardbm` instance.
modelardb_node = modelardb.connect(node)
```

## ModelarDB configuration
`ModelarDB` can be configured before the server is started using environment variables. A full list of the environment
variables is provided here. If an environment variable is not set, the specified default value will be used.

| **Variable**                                     | **Default** | **Description**                                                                                                                                                                                                                                                                                          |
|--------------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MODELARDBM_PORT                                  | 9998        | The port of the manager Apache Arrow Flight Server.                                                                                                                                                                                                                                                      |
| MODELARDBD_PORT                                  | 9999        | The port of the server Apache Arrow Flight Server.                                                                                                                                                                                                                                                       |
| MODELARDBD_IP_ADDRESS                            | 127.0.0.1   | The IP address of the Apache Arrow Flight Server.                                                                                                                                                                                                                                                        |
| MODELARDBD_MULTIVARIATE_RESERVED_MEMORY_IN_BYTES | 512 MB      | The amount of memory to reserve for storing multivariate time series.                                                                                                                                                                                                                                    |
| MODELARDBD_UNCOMPRESSED_RESERVED_MEMORY_IN_BYTES | 512 MB      | The amount of memory to reserve for storing uncompressed data buffers.                                                                                                                                                                                                                                   |
| MODELARDBD_COMPRESSED_RESERVED_MEMORY_IN_BYTES   | 512 MB      | The amount of memory to reserve for storing compressed data buffers.                                                                                                                                                                                                                                     |
| MODELARDBD_TRANSFER_BATCH_SIZE_IN_BYTES          | 64 MB       | The amount of data that must be collected before transferring a batch of data to the remote object store.                                                                                                                                                                                                |
| MODELARDBD_TRANSFER_TIME_IN_SECONDS              | Disabled    | The number of seconds between each transfer of data to the remote object store.                                                                                                                                                                                                                          |
| MODELARDBD_UNCOMPRESSED_DATA_BUFFER_CAPACITY     | 65536       | The capacity of each uncompressed data buffer as the number of elements in the buffer where each element is a timestamp and a value. Note that the resulting size of the buffer has to be a multiple of 64 bytes to avoid the actual capacity being larger than the requested due to internal alignment. |

## Docker
Two different [Docker](https://docs.docker.com/) environments are included to make it easy to experiment with the
different use cases of ModelarDB. The first environment sets up a single instance of `modelardbd` that only uses local
storage. Data can be ingested into this instance, compressed, and saved to local storage. The compressed data in local
storage can then be queried. The second environment covers the more complex use case of ModelarDB where multiple
instances of `modelardbd` are deployed in a cluster with a manager that is responsible for managing the cluster. A
single edge node and a single cloud node is set up in the cluster. Data can be ingested into the edge or cloud node,
compressed, and transferred to a remote object store. The compressed data in the remote object store can then be queried
through the cloud node or by directing the query through the manager node.

Note that since [Rust](https://www.rust-lang.org/) is a compiled language and a more dynamic ModelarDB configuration
might be needed, it is not recommended to use the [Docker](https://docs.docker.com/) environments during active
development of ModelarDB. They are however ideal to use for experimenting with ModelarDB or when developing
software that utilizes ModelarDB. Downloading [Docker Desktop](https://docs.docker.com/desktop/) is recommended to
make maintenance of the created containers easier.

### Single edge deployment
Once [Docker](https://docs.docker.com/) is set up, the single edge deployment can be started by running the following
command from the root of the ModelarDB repository:

```shell
docker-compose -p modelardb-single -f docker-compose-single.yml up
```

Note that `-p modelardb-single` is only used to name the project to make it easier to manage in [Docker
Desktop](https://docs.docker.com/desktop/). Once created, the container can be started and stopped using Docker Desktop
or by using the corresponding commands:

```console
docker-compose -p modelardb-single start
docker-compose -p modelardb-single stop
```

The single edge is running locally on port `9999` and can be accessed using the `modelardb` client or through Apache
Arrow Flight as described above. Tables can be created and data can be ingested, compressed, and saved to local disk.
The compressed data on local disk can then be queried.

### Cluster deployment
Once Docker is set up, the cluster deployment can be started by running the following command from the root of the
ModelarDB repository:

```shell
docker-compose -p modelardb-cluster -f docker-compose-cluster.yml up
```

Note that `-p modelardb-cluster` is only used to name the project to make it easier to manage in Docker Desktop. Once
created, the containers can be started and stopped using Docker Desktop or by using the corresponding commands:

```console
docker-compose -p modelardb-cluster start
docker-compose -p modelardb-cluster stop
```

The cluster deployment sets up a MinIO object store and a MinIO client is used to initialize the bucket `modelardb` in
the object store. MinIO can be administered through its [web interface](http://127.0.0.1:9001). The default username and
password, `minioadmin`, can be used to log in.

The cluster itself consists of a manager node, an edge node, and a cloud node. The manager node can be accessed using
the URL `grpc://127.0.0.1:9998`, the edge node using the URL `grpc://127.0.0.1:9999`, and the cloud node using the URL
`grpc://127.0.0.1:9997`. Tables can be created through the manager node and data can be ingested, compressed, and
transferred to the object store through the edge node or the cloud node. The compressed data in the MinIO object store
can then be queried through the cloud node or by directing the query through the manager node.
