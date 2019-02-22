#!/usr/bin/env bash
C_SRC=$1
if [[ ! -z ${C_SRC} ]]
then
    cppcheck --std=c++11 --language=c++ --inline-suppr --suppress=selfInitialization ${C_SRC}
    cpplint --extensions=cu,hpp ${C_SRC}
fi