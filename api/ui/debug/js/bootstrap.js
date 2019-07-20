//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

function initBootstrapViewer() {
    getOwnedTableShards();
}

function getOwnedTableShards() {
    Promise.all([
        promiseFromAjax({url: '/schema/tables'}),
        promiseFromAjax({url: '/dbg/shards'}),
    ]).then(function ([tables, shards]) {
        Promise.all(tables.map(table =>  {
            return promiseFromAjax({url: '/schema/tables/{0}'.format(table), type:'GET'})
        })).then(function (schemas) {
            Promise.all(schemas.flatMap(function (schema) {
                if (!schema.isFactTable) {
                    return [{table: schema.name, shard: 0}];
                } else {
                    return shards.map(function (shardID) {
                        return {table: schema.name, shard: shardID}
                    });
                }
            }).map(tableShard => {
                return promiseFromAjax({url: '/dbg/{0}/{1}'.format(tableShard.table, tableShard.shard)}, summary => {
                    return {
                        ...tableShard,
                        ...summary,
                    }
                })
            })).then(tableShardSummaries => {
                // console.log(tableShardSummaries);
                renderTableShardTable(tableShardSummaries);
            })
        });
    });
}

function renderTableShardTable(tableShardSummaries) {
    function renderBootstrapStatus(data, type, row) {
        const bootstrapStateMap = {
            0: {label: 'Not Started', color: 'red'},
            1: {label: 'Started', color: 'orange'},
            2: {label: 'Bootstrapped', color: 'green'},
        };
        return '<span style="color:'+bootstrapStateMap[data].color+'">'+bootstrapStateMap[data].label+'</span>';
    }

    function renderBootstrapStage(data, type, row) {
        const stageColorMap = {
            waiting:  'red',
            peercopy: 'orangered',
            preload:  'orange',
            recovery: 'yellowgreen',
            finished: 'green',
        };
        return '<span style="color:'+stageColorMap[data]+'">'+data+'</span>';
    }

    function renderProgress(data, type, row) {
        function getStyleClass(percent) {
            if (percent >= 100) {
                return "bg-success"
            } else if (percent >= 60) {
                return "bg-info"
            } else if (percent >= 30) {
                return "bg-warning"
            } else if (percent >= 0) {
                return "bg-danger"
            }
        }

        var percentFinished = 0;
        var numFinishedBatches = 0;
        var numPendingBatches = 0;
        var numColumns = data.numColumns;
        if (data.batches) {
            var pendingBatches = Object.keys(data.batches);
            numPendingBatches = pendingBatches.length;
            if (numPendingBatches > 0) {
                console.log(pendingBatches);
                numFinishedBatches = pendingBatches.map(batchID => {
                    var columnStatus = data.batches[batchID];
                    // if all columns are finished copying then batch is done
                    return columnStatus.filter(status => {return status !== 1}).length === numColumns ? 1 : 0;
                }).reduce((x, y) => x + y);
                percentFinished = numFinishedBatches * 100.0 / numPendingBatches;
            }
        }
        return '<div class="progress"> <div class="progress-bar {0} progress-bar-striped active" role="progressbar" aria-valuenow="{1}" aria-valuemin="0" aria-valuemax="100" style="width:{2}%"> {3} / {4} Batches Copied </div> </div>'.format(getStyleClass(percentFinished), percentFinished, percentFinished, numFinishedBatches, numPendingBatches);
    }

    var tableShardsTable = $('#table-shards-table').DataTable({
        paging: false,
        autoWidth: false,
        aoColumns: [
            {title: "Table", data: "table"},
            {title: "Shard", data: "shard"},
            {title: "Stage", data: "bootstrapDetails.stage", render: renderBootstrapStage},
            {title: "Peer Copy Progress", data: "bootstrapDetails", render: renderProgress},
            {title: "Status", data: "BootstrapState", render: renderBootstrapStatus},
        ],
        aaData: tableShardSummaries
    });

    // var tableColumnsTable = $('#table-columns-table').DataTable({
    //     paging: false,
    //     autoWidth: false,
    //     aoColumns: [
    //         {title: "Column", data: "column"},
    //         {title: "Status", data: "preloadMemory", type: "data-size", render: renderMem},
    //     ],
    //     aaData: []
    // });

    $('#table-columns-close').click(function(event){
        $('#table-columns').addClass("collapse");
        $("#table-shards").removeClass("col-md-6");
        $("#table-shards").addClass("col-md-12");
    });

    $('#table-shards-table').on('click', 'tr', function(event) {
        var data = tableShardsTable.row(this).data();
        tableColumnsTable.clear();
        tableColumnsTable.rows.add(data.columns).draw();
        $("#table-shards").removeClass("col-md-12");
        $("#table-shards").addClass("col-md-6");
        $('#table-columns').removeClass("collapse");
    });
}
