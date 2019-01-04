FROM nvidia/cuda:9.1-devel-ubuntu16.04

ENV GOPATH=/root/go
ENV PATH=${GOPATH}/bin:/usr/lib/go-1.9/bin:${PATH}
ENV LD_LIBRARY_PATH=:${LD_LIBRARY_PATH}/path/to/cuda/lib64:/path/to/aresdb/lib

# install add-apt-repository
RUN apt-get update
RUN apt-get install -y --reinstall software-properties-common

RUN add-apt-repository ppa:gophers/archive
RUN apt-get update
RUN apt-get install -y golang-1.9-go git

# clone aresdb repo and set up GOPATH
RUN mkdir -p $GOPATH/src/github.com
WORKDIR $GOPATH/src/github.com
RUN git clone https://github.com/uber/aresdb
RUN ln -sf $GOPATH/src/github.com/aresdb $HOME/aresdb
WORKDIR aresdb

# install go tools
RUN go get github.com/Masterminds/glide
