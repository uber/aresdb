#!/usr/bin/env bash
# install google test
output_dir=./build
rm -rf /tmp/googletest
git clone -b release-1.8.1 --single-branch https://github.com/google/googletest.git /tmp/googletest
pushd .
cd /tmp/googletest
# build makefile
cmake CMakeLists.txt
make
popd
mv /tmp/googletest/googletest ${output_dir}

# install cppcheck
sudo apt-get install cppcheck

# install cpplint
sudo apt-get install python-pip
sudo pip install cpplint
