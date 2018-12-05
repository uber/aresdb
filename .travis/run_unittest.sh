#!/usr/bin/env bash
set -ex

if [ -f "lib/.cached_commit" ]; then
  cachedLibCommit=$(cat lib/.cached_commit)
fi

cudaFileChanged=false
if [ ! -z "${cachedLibCommit}" ]; then
  numCUDAFilesChanged=$(git diff "${cachedLibCommit}" --name-only | grep -c -e "\\.hpp" -e "\\.h" -e "\\.cu" || true)
  if [ "${numCUDAFilesChanged}" -gt 0 ]; then
    cudaFileChanged=true
  fi
else
  cudaFileChanged=true
fi


if [ "${cudaFileChanged}" == "true" ]; then
  # clean up lib and cuda test when cuda file change found
  make clean
  make clean-cuda-test
else
  # touch files in lib and gtest to update the timestamp so that make will not treat lib objects as outdated 
  find lib -type f -exec touch {} +
  find gtest -type f -exec touch {} +
fi

# run test-cuda in host mode
make test-cuda -j

# build binary
make ares -j

# run test
ginkgo -r -cover

# update cached_commit
if [ "${cudaFileChanged}" == "true" ]; then
  currentCommit="$(git rev-list --no-merges -n 1 HEAD)"
  echo "${currentCommit}" > lib/.cached_commit
fi
