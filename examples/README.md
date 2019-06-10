## AresDB Examples

AresDB Examples provide examples code and dataset including table schema, data and queries for user to get familiar with how to use the system

### Prerequisites

Before running the examples code, a running aresdb server is needed.
Please refer to how to use [aresdb docker](../docker/README.md) to quickly start a aresdb server instance

### How To Use

Under this examples directory, each directory is a sample data set containing schema, data and queries, e.g. 1k_trips 

To run the examples code:
```
go build examples.go
./examples tables --dataset 1k_trips
./examples data --dataset 1k_trips
./examples query --dataset 1k_trips
```
