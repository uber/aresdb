import React, { Component } from 'react';
import update from 'immutability-helper';
import './App.css';
import {Tab,Tabs,DropdownButton,MenuItem,Button} from 'react-bootstrap';
import JSONInput from 'react-json-editor-ajrm';
import 'react-table/react-table.css'
import ReactTable from 'react-table'
import FoldableTableHOC from 'react-table/lib/hoc/foldableTable'
import locale from 'react-json-editor-ajrm/locale/en';

const baseUrl = "";
// const baseUrl = "http://localhost:6708";
const FoldableTable = FoldableTableHOC(ReactTable);

const stateMapping = {
    0: "initializing",
    1: "available",
    2: "leaving"
};

const stateColorMapping = {
    0: '#ffbf00',
    1: '#57d500',
    2: '#ff2e00',
};

const statusColorMapping = {
    active:  '#57d500',
    down: '#ff2e00'
};

class App extends Component {
  constructor(props) {
      super(props);
      this.state = {
          tables: [],
          jobs: [],
          instances:[],
          assignments:[],
          namespaces: [],
          namespace: "",
          tableData: {},
          tableDataModified: {},
          jobData: {},
          jobDataModified: {},
          instanceData:{},
          instanceDataModified:{},
          assignmentData: {},
          assignmentDataModified: {},
          placement: {},
      };
      this.pushTable = this.pushTable.bind(this);
      this.pushJob = this.pushJob.bind(this);
  }
  componentDidMount() {
      this.fetchNamespaces();
      this.interval = setInterval(() => {
          this.fetchNamespaces();
          if (this.state.namespace) {
              this.fetchPlacement(this.state.namespace);
              this.fetchInstances(this.state.namespace);
          }
      }, 5000);
  }

  componentWillUnmount() {
    clearInterval(this.interval);
  }

  fetchNamespaces() {
      const url = baseUrl + "/namespaces";
      fetch(url).then(
          (result) => {
              return result.json();
          }
      ).then((j) => {
          const newState = update(this.state, {
              namespaces: {$set: j},
          });
          this.setState(newState);
      });
  }

  fetchTables(namespace) {
      const url = baseUrl + "/schema/"+namespace+"/tables";
      fetch(url).then(
          (result) => {
              return result.json();
          }
      ).then(j=>{
          const newState = update(this.state, {
              tables: {$set: j.map(t=>{return t.name})},
          });
          this.setState(newState);
      });
  }

    fetchTable(tableName) {
        const url = baseUrl + "/schema/"+this.state.namespace+"/tables/"+tableName;
        fetch(url).then(
            (result) => {
                return result.json();
            }
        ).then(j=>{
            const newState = update(this.state, {
                tableData: {$set: j},
            });
            this.setState(newState);
        });
    }

    pushTable() {
        this.pushChangedObject("table");
    }

    pushJob() {
        this.pushChangedObject("job");
    }

    pushChangedObject(objectType) {
      let changedObject = undefined;
      let url = baseUrl;
      switch (objectType) {
          case "table":
              changedObject = this.state.tableDataModified.jsObject;
              url += "/schema/"+this.state.namespace+"/tables/"+changedObject['name'];
              break;
          case "job":
              changedObject = this.state.jobDataModified.jsObject;
              url += "/config/"+this.state.namespace+"/jobs/"+changedObject['job'];
              break;
          default:
              throw new Error("invalid type to push")
      }
        console.log("pushing"+objectType+"change", changedObject);

        fetch(url, {
            method: 'PUT',
            body: JSON.stringify(changedObject),
            headers: {
                'Content-Type': 'application/json'
            }
        }).then(
            (result) => {
                if (!result.ok) {throw result;}
                return result.json();
            }
        ).then(j=>{
            let newState = undefined;
            switch (objectType) {
                case "table":
                    newState = update(this.state, {
                        tableData: {$set: j},
                    });
                    break;
                case "job":
                    newState = update(this.state, {
                        jobData: {$set: j},
                    });
                    break;
                default:
                    throw new Error("invalid type to push")
            }
            this.setState(newState);
            alert(objectType+" change pushed successfully!");
        }).catch(
            (error) => {
                alert("failed to push"+objectType+"change: " + error.statusText);
                console.log(error);
                window.location.reload();
            }
        );
    }

    fetchJobs(namespace) {
        const url = baseUrl + "/config/"+namespace+"/jobs";
        fetch(url).then(
            (result) => {
                return result.json();
            }
        ).then(j=>{
            const newState = update(this.state, {
                jobs: {$set: j.map(t=>{return t.job})},
            });
            this.setState(newState);
        });
    }

    fetchJob(jobName) {
        const url = baseUrl + "/config/"+this.state.namespace+"/jobs/"+jobName;
        fetch(url).then(
            (result) => {
                return result.json();
            }
        ).then(j=>{
            const newState = update(this.state, {
                jobData: {$set: j},
            });
            this.setState(newState);
        });
    }

    fetchInstances(namespace) {
        const url = baseUrl + "/membership/"+namespace+"/instances";
        fetch(url).then(
            (result) => {
                return result.json();
            }
        ).then(j=>{
            var instances = [];
            if (j && j.instances) {
                instances = Object.keys(j && j.instances);
            }
            const newState = update(this.state, {
                instances: {$set: instances},
            });
            this.setState(newState);
        });
    }

    fetchInstance(jobName) {
        const url = baseUrl + "/membership/"+this.state.namespace+"/instances/"+jobName;
        fetch(url).then(
            (result) => {
                return result.json();
            }
        ).then(j=>{
            const newState = update(this.state, {
                instanceData: {$set: j},
            });
            this.setState(newState);
        });
    }

    fetchAssignments(namespace) {
        const url = baseUrl + "/assignment/"+namespace+"/assignments";
        fetch(url).then(
            (result) => {
                return result.json();
            }
        ).then(a=>{
            const newState = update(this.state, {
                assignments: {$set: a.map(a=>{return a.subscriber})},
            });
            this.setState(newState);
        });
    }

    fetchAssignment(subscriberName) {
        const url = baseUrl + "/assignment/"+this.state.namespace+"/assignments/"+subscriberName;
        fetch(url).then(
            (result) => {
                return result.json();
            }
        ).then(j=>{
            const newState = update(this.state, {
                assignmentData: {$set: j},
            });
            this.setState(newState);
        });
    }

    fetchPlacement(namespace) {
        const url = baseUrl + "/placement/"+namespace+"/datanode";
        fetch(url).then(
            (result) => {
                return result.json();
            }
        ).then(p=>{
            const newState = update(this.state, {
                placement: {$set: p},
            });
            this.setState(newState);
        });
    }

    getAllShards(placement, instances) {
        var activeInstanceMap = {};
        instances.forEach(instance => {
            activeInstanceMap[instance] = true;
        });
        var instancesIDs = placement.instances ? Object.keys(placement.instances) : [];
        return instancesIDs.flatMap(id => {
            var instance = placement.instances[id];
            return instance.shards.map(shard => {
                return {
                    ...instance,
                    status: activeInstanceMap[instance.id] ? 'active' : 'down',
                    shardID: shard.id ? shard.id : 0,
                    state: shard.state ? shard.state : 0,
                }
            });
        });
    }

  render() {

    return (
      <div className="App">
        <header className="App-header">
            <DropdownButton
                bsStyle={"default"}
                title={"namespaces"}
                id="namespaces-dropdown"
            >
                {this.state.namespaces.map((namespace, index) => {
                    return <MenuItem
                        key={index}
                        eventKey={index}
                        onClick={()=>{
                            const newState = update(this.state, {
                               namespace: {$set: namespace},
                                tableData: {$set: {}}
                            });
                            this.setState(newState);
                            this.fetchTables(namespace);
                            this.fetchJobs(namespace);
                            this.fetchInstances(namespace);
                            this.fetchAssignments(namespace);
                            this.fetchPlacement(namespace);
                        }}
                    >{namespace}</MenuItem>
                })}
            </DropdownButton> <div>Viewing namespace: {this.state.namespace}</div>
            <Tabs defaultActiveKey={1} id="uncontrolled-tab-example">
                <Tab eventKey={1} title="Table Schema">
                    <DropdownButton
                        bsStyle={"default"}
                        title={"tables"}
                        id="tables-dropdown"
                    >
                        {this.state.tables.map((table, index) => {
                            return <MenuItem
                                        key={index}
                                        eventKey={index}
                                        onClick={()=>{
                                            this.fetchTable(table);
                                        }}
                            >{table}</MenuItem>
                        })}
                    </DropdownButton>
                    <JSONInput
                        id='schema_json'
                        placeholder={this.state.tableData}
                        locale={locale}
                        onChange={(data) => {
                            const newState = update(this.state, {
                                tableDataModified: {$set: data},
                            });
                            this.setState(newState);
                        }}
                    />
                    <Button bsStyle="primary" bsSize="medium" active onClick={this.pushTable}>Push Change</Button>
                </Tab>
                <Tab eventKey={2} title="Job Config">
                    <DropdownButton
                        bsStyle={"default"}
                        title={"jobs"}
                        id="jobs-dropdown"
                    >
                        {this.state.jobs.map((job, index) => {
                            return <MenuItem
                                key={index}
                                eventKey={index}
                                onClick={()=>{
                                    this.fetchJob(job);
                                }}
                            >{job}</MenuItem>
                        })}
                    </DropdownButton>
                    <JSONInput
                        id='job_json'
                        placeholder={this.state.jobData}
                        locale={locale}
                        onChange={(data) => {
                            const newState = update(this.state, {
                                jobDataModified: {$set: data},
                            });
                            this.setState(newState);
                        }}
                    />
                    <Button variant="outline-danger" bsStyle="primary" bsSize="medium" active onClick={this.pushJob}>Push Change</Button>
                </Tab>
                <Tab eventKey={3} title="Cluster Info">
                    <DropdownButton
                        bsStyle={"default"}
                        title={"instances"}
                        id="instances-dropdown"
                    >
                        {this.state.instances.map((instance, index) => {
                            return <MenuItem
                                key={index}
                                eventKey={index}
                                onClick={()=>{
                                    this.fetchInstance(instance);
                                }}
                            >{instance}</MenuItem>
                        })}
                    </DropdownButton>
                    <JSONInput
                        id='cluster_json'
                        placeholder={this.state.instanceData}
                        locale={locale}
                    />
                </Tab>
                <Tab eventKey={4} title="Assignments">
                    <DropdownButton
                        bsStyle={"default"}
                        title={"assignments"}
                        id="assignments-dropdown"
                    >
                        {this.state.assignments.map((assignment, index) => {
                            return <MenuItem
                                key={index}
                                eventKey={index}
                                onClick={()=>{
                                    this.fetchAssignment(assignment);
                                }}
                            >{assignment}</MenuItem>
                        })}
                    </DropdownButton>
                    <JSONInput
                        id='cluster_json'
                        placeholder={this.state.assignmentData}
                        locale={locale}
                    />
                </Tab>
                <Tab eventKey={5} title="Placement">
                    <FoldableTable
                        data={this.getAllShards(this.state.placement, this.state.instances)}
                        filterable={true}
                        className="-striped -highlight"
                        defaultPageSize={25}
                        columns={[{
                                Header: 'Shard ID',
                                accessor:'shardID'
                            },{
                                Header: 'Instance ID',
                                accessor:'id',
                            },{
                                Header: 'Instance Details',
                                foldable: true,
                                columns: [{
                                    Header: "Zone",
                                    accessor: 'zone',
                                }, {
                                    Header: 'Isolation Group',
                                    accessor: 'isolation_group',
                                }, {
                                    Header: 'Weight',
                                    accessor: 'weight',
                                }, {
                                    Header: 'Endpoint',
                                    accessor: 'endpoint',
                                }, {
                                    Header: 'Status',
                                    accessor: 'status',
                                    Cell: row => (
                                        <span style={{
                                            color: statusColorMapping[row.value]
                                        }}>{row.value}</span>
                                    )
                                }],
                            },{
                                Header: "State",
                                accessor: "state",
                                Cell: row => (
                                    <span>
                                        <span style={{
                                            color: stateColorMapping[row.value],
                                            transition: 'all .3s ease'
                                        }}>&#x25cf;</span>
                                        {stateMapping[row.value]}
                                    </span>
                                ),
                            }]}
                    />
                </Tab>
            </Tabs>
        </header>
      </div>
    );
  }
}

export default App;
