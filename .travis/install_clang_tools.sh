#!/usr/bin/env bash
set -ex
# install google test
output_dir=./build
cmake_version=3.12.0
# install cppcheck

sudo apt-get update
sudo apt-get install cppcheck

# install cpplint
sudo apt-get install python-pip
sudo pip install cpplint

# install cmake
pushd .
cd /tmp
wget https://github.com/Kitware/CMake/releases/download/v${cmake_version}/cmake-${cmake_version}-Linux-x86_64.tar.gz
tar xzf cmake-${cmake_version}-Linux-x86_64.tar.gz
popd
mv /tmp/cmake-${cmake_version}-Linux-x86_64 ./build/cmake
