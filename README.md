[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![license](https://img.shields.io/github/license/uber/aresdb.svg)](LICENSE) [![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fuber%2Faresdb.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fuber%2Faresdb?ref=badge_shield)

<br>
<p align="center"><img src="docs/logo.png" alt="AresDB Logo" width="220" height="209"></p>

AresDB
==============
AresDB is a GPU-powered real-time analytics storage and query engine. It features low query latency, high data freshness and highly efficient in-memory and on disk storage management. Please see AresDB's features, architecture design described in the [Uber Engineering Blog](https://eng.uber.com/aresdb/).

This repo contains the source code of AresDB and debug UI.

Legal Note
----------
AresDB requires the CUDA Toolkit. Please ensure you read, acknowledge, and accept the [CUDA End User License Agreement](https://docs.nvidia.com/cuda/eula/index.html).

Getting started
---------------
To get AresDB:

```
git clone --recursive https://github.com/uber/aresdb.git $GOPATH/src/github.com/uber/aresdb
```

NVIDIA Driver and CUDA Setup
----------------------------
AresDB needs [NVIDIA driver](https://www.nvidia.com/Download/index.aspx) version >= 390.48 and [CUDA](https://developer.nvidia.com/cuda-91-download-archive) version 9.1.

Environment Variables
---------------------
Run the following to make sure the following environment variables are correctly set:
```
export PATH=/path/to/cuda/bin:${PATH}
export LD_LIBRARY_PATH=/path/to/cuda/lib64:/path/to/aresdb/lib:${LD_LIBRARY_PATH}
export PKG_CONFIG_PATH=${LD_LIBRARY_PATH}/pkgconfig:${PKG_CONFIG_PATH}
```


Language Requirements
---------------------
Building and running AresDB requires:
* [golang](https://golang.org/) 1.11+
* C++ compiler that support c++14
* [cmake](https://cmake.org/download/) 3.12+
* [nvcc](https://docs.nvidia.com/cuda/cuda-compiler-driver-nvcc/index.html) version 9.1

Configure
---------
Run following commands to generate makefile:
```
cmake -DQUERY_MODE=DEVICE .
```

Alternatively, if you want to run the query in CPU mode, run following commands:
```
cmake -DQUERY_MODE=HOST .
```

Local Test
----------
AresDB is written in C++ (query engine) and Golang (mem store, disk store and other query components). Because of this, we break testing into two parts:
### Test Golang Code
#### Ginkgo
We use [Ginkgo](https://github.com/onsi/ginkgo) as the test framework for running the Golang unit test and coverage. Install Ginkgo first and run
```
make test-golang
```

### Test C++ Code
#### google-test
We use [google-test](https://github.com/google/googletest) as the test framework to test C++ code. Install google-test and set the environment variable, GTEST_ROOT, to the installed location.

After you have installed properly, run
```
make test-cuda
```

Run AresDB Server
-----------------
The following command will start an AresDB server locally. You can start to query the server using a curl command or [swagger](https://github.com/uber/aresdb/wiki/Swagger) page.
```
make run_server
```

Run AresDB Docker
-----------------
Please read the [Docker](docker/README.md) page.

Documentation
--------------

Interested in learning more about AresDB? Read the [blog post](https://eng.uber.com/aresdb/)

License
-------
Apache 2.0 License, please see [LICENSE](LICENSE) for details.

[ci-img]: https://travis-ci.com/uber/aresdb.svg?branch=master
[ci]: https://travis-ci.com/uber/aresdb
[cov-img]: https://codecov.io/gh/uber/aresdb/branch/master/graph/badge.svg
[cov]: https://codecov.io/gh/uber/aresdb

