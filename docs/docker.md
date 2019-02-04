## AresDB Docker
To facilitate the installation process, we have published the [Dockerfile](../Dockerfile) so
you can get AresDB running in a docker container.

### Prerequisites

* [nvidia-driver](https://github.com/NVIDIA/nvidia-docker/wiki/Frequently-Asked-Questions#how-do-i-install-the-nvidia-driver)
* A [supported version](https://github.com/NVIDIA/nvidia-docker/wiki/Frequently-Asked-Questions#which-docker-packages-are-supported) of [Docker]
* [nvida-docker](https://github.com/NVIDIA/nvidia-docker)


### Build

```bash
$ mkdir aresdb-docker
$ wget -O aresdb-docker/Dockerfile https://raw.githubusercontent.com/uber/aresdb/master/Dockerfile
$ docker build -t aresdb:latest aresdb-docker
```

### Run AresDB
```bash
nvidia-docker run -p 9374:9374/tcp -p 43202:43202/tcp -it aresdb:latest
root@9e4c5150659c# cd ~/go/src/github.com/aresdb
root@9e4c5150659c# make run_server 
```
Point your browser to :`http://server_ip_address:9374/swagger/`

This command will compile AresDB and map the ports in the docker container on the docker host.
Refer to [Swagger](https://github.com/uber/aresdb/wiki/Swagger) to start interactiving with AresDB.
