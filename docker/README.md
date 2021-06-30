## AresDB Docker
To facilitate the installation process, we have published the [Dockerfile](./Dockerfile) so
you can get AresDB running in a docker container.

### Prerequisites

* [nvidia-driver](https://github.com/NVIDIA/nvidia-docker/wiki/Frequently-Asked-Questions#how-do-i-install-the-nvidia-driver)
* A [supported version](https://github.com/NVIDIA/nvidia-docker/wiki/Frequently-Asked-Questions#which-docker-packages-are-supported) of [Docker]
* [nvidia-docker](https://github.com/NVIDIA/nvidia-docker)


### Build

```bash
$ mkdir aresdb-docker
$ wget -O aresdb-docker/Dockerfile https://raw.githubusercontent.com/uber/aresdb/master/docker/Dockerfile
$ docker build -t aresdb:latest aresdb-docker
```

### Run AresDB
```
nvidia-docker run -p 9374:9374/tcp -p 43202:43202/tcp -it aresdb:latest
root@9e4c5150659c:~/go/src/github.com/aresdb# make run_server >> ./log/aresdb.log 2>&1 &
```

This command will compile AresDB, run the server in background and map the ports in the docker container on the docker host.
Refer to [Swagger](https://github.com/uber/aresdb/wiki/Swagger) to start interactiving with AresDB.
