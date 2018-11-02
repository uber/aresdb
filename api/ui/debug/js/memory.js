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

var pieChart = null;
jQuery.fn.dataTable.ext.type.order['data-size-pre'] = function (data) {
    var matches = data.match(/^(\d+\.\d+)?\s*([a-z]+)/i);
    var multipliers = {
	    b: 1,
        kb: 1024,
        mb: 1048576,
        gb: 1073741824
    };
 
    if (matches) {
        var multiplier = multipliers[matches[2].toLowerCase()];
        return parseFloat(matches[1]) * multiplier;
    } else {
        return -1;
    }
};

function initHostMemoryExplorer() {
    $.ajax({
            url: "/dbg/host-memory",
            success: function (body) {
                initTableShardList(body);
            },
            error: function (xhr) {
                alert(xhr.responseText)
            }
        }
    )
}

var tableShardData = [];
var rawData;
function initTableShardList(data) {
    rawData = data;
    renderMemoryTable();
}

function processRawData(data) {
    tableShardData = [];
    Object.keys(data).forEach(function(tableShardKey) {
        var index = tableShardKey.lastIndexOf("_");
        var table = tableShardKey.substring(0, index);
        var shard = tableShardKey.substring(index+1);

        var preloadMemory = 0;
        var nonPreloadMemory = 0;
        var liveMemory = 0;
        var primaryKeyMemory = data[tableShardKey].pk;
        Object.keys(data[tableShardKey].cols).forEach(function(column) {
            preloadMemory += data[tableShardKey].cols[column].preloaded;
            nonPreloadMemory += data[tableShardKey].cols[column].nonPreloaded;
            liveMemory += data[tableShardKey].cols[column].live;
        });

        var tableShardItem = {
            table: table,
            shard: shard,
            preloadMemory: preloadMemory,
            nonPreloadMemory: nonPreloadMemory,
            liveMemory: liveMemory,
            primaryKeyMemory: primaryKeyMemory,
            total: preloadMemory + nonPreloadMemory + liveMemory + primaryKeyMemory,
            columns: processColumnMemoryData(tableShardKey, data[tableShardKey].cols)
        };

        tableShardData.push(tableShardItem);
    });
}

function processColumnMemoryData(tableShardKey, columns) {
    var columnMemoryData = [];
    Object.keys(columns).forEach(function(column) {
        var preloadMemory = columns[column].preloaded;
        var nonPreloadMemory = columns[column].nonPreloaded;
        var liveMemory = columns[column].live;
        var total = preloadMemory + nonPreloadMemory + liveMemory;
        columnMemoryData.push({
            column: column,
            preloadMemory: preloadMemory,
            nonPreloadMemory: nonPreloadMemory,
            liveMemory: liveMemory,
            total: total
        });
    });
    return columnMemoryData;
}

function drawPieChat(name, data, labels) {
    if (pieChart != null) {
        pieChart.destroy();
    }

    var ctx = document.getElementById("chart").getContext('2d');
    pieChart = new Chart(ctx, {
        type: 'pie',
        data: {
            datasets: [{
                label: name,
                data: data,
                backgroundColor: palette('tol-rainbow', data.length).map(function(hex) {
                    return '#' + hex;
                })
            }],
            labels: labels,
            options: {
                tooltips: {
                    callbacks: {
                        label: function(tooltipItem, data) {
                            var datasetLabel = '';
                            var label = data.labels[tooltipItem.index];
                            return data.datasets[tooltipItem.datasetIndex].data[tooltipItem.index];
                        }
                    }
                }
            }
        }
    });
    $('#chart-container').show();
}

function renderMemoryTable() {
    processRawData(rawData);

    function renderMem(data, type, row) {
       var gb = 1 << 30;
       var mb = 1 << 20;
       var kb = 1 << 10;
       if (data > gb) {
         return '' + (data / gb).toFixed(2) + ' GB';
       } else if (data > mb) {
         return '' + (data / mb).toFixed(2) + ' MB';
       } else if (data > kb) {
         return '' + (data / kb).toFixed(2) + ' KB';
       }
       return '' + data.toFixed(2) + ' B'; 
    }

    var tableShardsTable = $('#table-shards-table').DataTable({
        paging: false,
        autoWidth: false,
        aoColumns: [
            {title: "Table", data: "table"},
            {title: "Shard", data: "shard"},
            {title: "Preloaded", data: "preloadMemory", type: "data-size", render: renderMem},
            {title: "NonPreloaded", data: "nonPreloadMemory", type: "data-size", render: renderMem},
            {title: "Live", data: "liveMemory", type: "data-size", render: renderMem},
            {title: "PrimaryKey", data: "primaryKeyMemory", type: "data-size", render: renderMem},
            {title: "Total", data: "total", type: "data-size", render: renderMem}
        ],
        aaData: tableShardData
    });

    var tableColumnsTable = $('#table-columns-table').DataTable({
        paging: false,
        autoWidth: false,
        aoColumns: [
            {title: "Column", data: "column"},
            {title: "Preloaded", data: "preloadMemory", type: "data-size", render: renderMem},
            {title: "NonPreloaded", data: "nonPreloadMemory", type: "data-size", render: renderMem},
            {title: "Live", data: "liveMemory", type: "data-size", render: renderMem},
            {title: "Total", data: "total", type: "data-size", render: renderMem}
        ],
        aaData: []
    });

    $('#table-columns-close').click(function(event){
        $('#table-columns').addClass("collapse");
        $("#table-shards").removeClass("col-md-6");
        $("#table-shards").addClass("col-md-12");
    });

    $('#table-shards-table').on('click', 'tr', function(event) {
        var data = tableShardsTable.row(this).data();
        var chartData = [];
        var chartLabels = [];
        if (!data) {
          return;
        }
        data.columns.forEach(function(col) {
            chartData.push(col.total);
            chartLabels.push(col.column);
        });

        drawPieChat('Memory By Columns', chartData, chartLabels);

        tableColumnsTable.clear();
        tableColumnsTable.rows.add(data.columns).draw();
        $("#table-shards").removeClass("col-md-12");
        $("#table-shards").addClass("col-md-6");
        $('#table-columns').removeClass("collapse");
    });

    $('#table-columns-table').on('click', 'tr', function(event) {
        var data = tableColumnsTable.row(this).data();
        if (!data) {
          return;
        }
        var chartData = [data.preloadMemory, data.nonPreloadMemory, data.liveMemory];
        var chartLabels = ['preloaded', 'nonPreloaded', 'live'];
        drawPieChat('Column Memory Detail', chartData, chartLabels);
    });

    $('#chart-container').hide();
}
