AresDB
==============
AresDB is a GPU-based analytics engine with storage and query. The goal is to develop a real time analytics system with fresh data and interactive response time at low cost.
This repo contains the source code of AresDB and debug UI.

Legal Note
----------
AresDB requires CUDA Toolkit. Please ensure you read, acknowledge, and accept the [CUDA End User License Agreement](https://docs.nvidia.com/cuda/eula/index.html).

Getting started
---------------
To get AresDB:

```
git clone git@github.com:uber/AresDB.git $GOPATH/src/github.com/uber/AresDB.git
```

NVIDIA Driver and CUDA Setup
----------------------------
AresDB needs [NVIDIA driver](https://www.nvidia.com/Download/index.aspx) version >= 390.48 and [CUDA](https://developer.nvidia.com/cuda-91-download-archive) version 9.1.

Environment Variables
---------------------
Make sure following environment variables are correctly set:
```
export PATH=/path/to/cuda/bin:${PATH}
export LD_LIBRARY_PATH=/path/to/cuda/lib64:/path/to/aresdb/lib:${LD_LIBRARY_PATH}
```


Language Requirements
---------------------
Building and running AresDB requires:
* [golang](https://golang.org/) 1.9+
* C++ compiler that support c++11
* [nvcc](https://docs.nvidia.com/cuda/cuda-compiler-driver-nvcc/index.html) version 9.1

Build
-----
Following dependencies need to be installed before build the binary.

### glide
We use [glide](https://glide.sh) to manage Go dependencies. Please make sure `glide` is in your PATH before you attempt to build.

###

Local Test
----------
AresDB is written in C++(query engine) and Golang (mem store, disk store, disk store and other query components.) So we break test into 2 parts:
### Test Golang Code
#### ginkgo
We use [ginkgo](https://github.com/onsi/ginkgo) as the test framework for running golang unit test and coverage, install it first and run
```
make test
```

### Test C++ Code
#### google-test
We use [google-test](https://github.com/google/googletest) as the test framework to test c++ code, install it first and set environment variable GTEST_ROOT to the installed location.

And then run
```
make test-cuda
```

Run AresDB Server
-----------------
Following command will start a AresDB server locally, you can start query it using curl command or swagger page
```
make run
```

Contributing
------------

We'd love your help in making AresDB great. If you find a bug or need a new feature, open an issue and we will respond as fast as we can. If you want to implement new feature(s) and/or fix bug(s) yourself, open a pull request with the appropriate unit tests and we will merge it after review.

**Note:** All contributors also need to fill out the [Uber Contributor License Agreement](http://t.uber.com/cla) before we can merge in any of your changes.

Documentation
--------------

Interested in learning more about AresDB? Read the blog post:
[TBD](TBD)

License
-------
Apache 2.0 License, please see [LICENSE](LICENSE) for details.
