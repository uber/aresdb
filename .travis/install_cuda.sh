#!/usr/bin/env bash
set -ex
wget http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1404/x86_64/${CUDA_PKG}
sudo apt-key adv --fetch-keys http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1404/x86_64/7fa2af80.pub
sudo dpkg -i ${CUDA_PKG}
rm ${CUDA_PKG}
sudo apt-get -y update
sudo apt-get install -y --no-install-recommends  cuda-core-${CUDA_VERSION}  cuda-cudart-dev-${CUDA_VERSION}  cuda-cublas-dev-${CUDA_VERSION} cuda-curand-dev-${CUDA_VERSION}
sudo ln -sf /usr/local/cuda-${CUDA_VERSION} /usr/local/cuda