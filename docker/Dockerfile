FROM nvidia/cuda:9.1-devel-ubuntu16.04

ENV GOPATH=/root/go
ENV UBER_GITHUB_DIR=$GOPATH/src/github.com/uber
ENV ARESDB_PATH=$UBER_GITHUB_DIR/aresdb
ENV PATH=${GOPATH}/bin:/usr/lib/go-1.9/bin:${PATH}
ENV LD_LIBRARY_PATH=:${LD_LIBRARY_PATH}:/usr/local/cuda/lib64:${ARESDB_PATH}/lib

# install add-apt-repository
RUN apt-get update --fix-missing
RUN apt-get install -y --reinstall software-properties-common

RUN add-apt-repository ppa:gophers/archive
RUN apt-get update
RUN apt-get install -y golang-1.9-go git npm wget

#install cmake 3.12
ARG CMAKE_VERSION=3.12.4
WORKDIR /tmp
RUN wget https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.tar.gz
RUN tar -xzvf cmake-${CMAKE_VERSION}.tar.gz
WORKDIR cmake-${CMAKE_VERSION}
RUN ./bootstrap && make -j && sudo make install && cmake --version

WORKDIR $HOME
# clone aresdb repo and set up GOPATH
RUN mkdir -p $UBER_GITHUB_DIR
WORKDIR $UBER_GITHUB_DIR
RUN git clone https://github.com/uber/aresdb
RUN ln -sf $UBER_GITHUB_DIR/aresdb $HOME/aresdb
WORKDIR aresdb
RUN cmake .
RUN make npm-install
RUN mkdir log

# install go tools
RUN go get github.com/Masterminds/glide
RUN make aresd -j