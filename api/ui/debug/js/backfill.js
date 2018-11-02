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

var upsertbatchTable;

function initSchedulerViewer() {
    // Disable null value warning from data table.
    $.fn.dataTable.ext.errMode = 'none';

    $.ajax({
            url: "/dbg/jobs/backfill",
            success: function (body) {
                var runningJobData = []
                var pastRunsData = []
                for (var key in body) {
                    var strs = key.split("|")
                    var row = body[key]
                    row['table'] = strs[0]
                    row['shard'] = strs[1]
                    row['type'] = strs[2]
                    if ('lastDuration' in row) {
                        row['lastDuration'] = row['lastDuration'].toDuration()
                    }

                    if ('lockDuration' in row) {
                        row['lockDuration'] = row['lockDuration'].toDuration()
                    }

                    if (row['status'] == 'running') {
                        runningJobData.push(row)
                    } else {
                        // Also get stats like current backfill buffer size and backfilling size, etc.
                        $.ajax({
                                url: "/dbg/{0}/{1}".format(row['table'], row['shard']),
                                success: function (body) {
                                    var backfillMgr = body["liveStore"]["backfillManager"];
                                    if (backfillMgr) {
                                        row["numRecords"] = backfillMgr["numRecords"];
                                        row["currentBufferSize"] = backfillMgr["currentBufferSize"];
                                        row["backfillingBufferSize"] = backfillMgr["backfillingBufferSize"];
                                        row["maxBufferSize"] = backfillMgr["maxBufferSize"];
                                        row["numUpsertBatches"] = backfillMgr["numUpsertBatches"];
                                    }
                                },
                                error: function (xhr) {
                                    alert(xhr.responseText);
                                },
                                async: false
                            }
                        )
                        pastRunsData.push(row);
                    }
                }
                initRunningJobTable(runningJobData)
                initPastRunTable(pastRunsData)
            },
            error: function (xhr) {
                alert(xhr.responseText)
            }
        }
    );

    // Init table selector.
    $('#table-selector').select2({
        ajax: {
            url: "/schema/tables",
            dataType: 'json',
            quietMillis: 50,
            processResults: function (data) {
                return {
                    results: $.map(data, function (item, idx) {
                        return {
                            text: item,
                            id: idx + 1,
                        }
                    })
                };
            }
        },
        width: 'resolve'
    }).on('change', function () {
        refreshBackfillQueue();
    });

    // Init shard selector.
    $('#shard-selector').select2({
        data: [
            {
                "id": 0,
                "text": 0,
            }
        ]
    }).on('change', function (e) {
        refreshBackfillQueue();
    });
}

function refreshBackfillQueue() {
    var table = $('#table-selector').select2('data')[0].text;
    var shard = $('#shard-selector').select2('data')[0].text;
    if ($('#upsertbatch-selector').select2()) {
        $('#upsertbatch-selector').empty();
        $('#upsertbatch-selector').select2("destroy");
    }

    $('#upsertbatch-selector').select2({
        ajax: {
            url: "/dbg/{0}/{1}".format(table, shard),
            cache: true,
            dataType: 'json',
            quietMillis: 50,
            processResults: function (data) {
                var results = [];
                var numUpsertBatches = data.liveStore.backfillManager.numUpsertBatches;
                for (var i = 0; i < numUpsertBatches; i++) {
                    results.push({
                        text: i,
                        id: i + 1
                    });
                }

                return {
                    results: results
                };
            }
        },
        width: '100px',
        minimumResultsForSearch: -1
    }).on('change', function (e) {
        refreshUpsertBatchTable();
    });
}

function refreshUpsertBatchTable() {
    var table = $("#table-selector").select2('data')[0].text;
    var shard = $("#shard-selector").select2('data')[0].text;
    var upsertBatch = $("#upsertbatch-selector").select2('data')[0].text;

    $.ajax({
        url: "/dbg/{0}/{1}/backfill-manager/upsertbatches/{2}".format(table, shard, upsertBatch),
        success: function (body) {
            var columns = body.columnNames.map(function (name) {
                    return {"title": name}
                }
            );

            // Need to explicitly destroy old data table.
            if (upsertbatchTable) {
                upsertbatchTable.destroy();
                $('#upsertbatch-table').empty();
            }

            upsertbatchTable = $('#upsertbatch-table').DataTable({
                "serverSide": true,
                "processing": true,
                "paging": true,
                "searching": false,
                "pageLength": 20,
                "lengthMenu": [[1, 10, 25, 50, 100], [1, 10, 25, 50, 100]],
                "columns": columns,
                "ajax": {
                    "type": "GET",
                    "url": "/dbg/{0}/{1}/backfill-manager/upsertbatches/{2}".format(table, shard, upsertBatch),
                    "dataType": "json",
                    "contentType": 'application/json'
                }
            });
        },
        error: function (error) {
            alert('error: ' + eval(error));
        }
    });
}

function submitBackfillJob(table, shard) {
    var url = "/dbg/{0}/{1}/backfill".format(table, shard)
    $.ajax({
            url: url,
            method: "POST",
            dataType: 'json',
            success: function (body) {
                alert(body);
                reloadCurrentTab();
            },
            error: function (xhr) {
                alert(xhr.responseText);
            }
        }
    )
}

function initRunningJobTable(data) {
    $('#running-job-table').DataTable({
        paging: false,
        searching: false,
        aoColumns: [
            {title: "Table", data: "table"},
            {title: "Shard", data: "shard", type: "num"},
            {title: "Type", data: "type"},
            {title: "Stage", data: "stage"},
            {title: "Current", data: "current", type: "num"},
            {title: "Total", data: "total", type: "num"},
        ],
        aaData: data,
    });
}

function initPastRunTable(data) {
    $('#past-runs-table').DataTable({
        paging: true,
        searching: true,
        pageLength: 20,
        lengthMenu: [[1, 10, 25, 50, 100], [1, 10, 25, 50, 100]],
        aoColumns: [
            {title: "Table", data: "table"},
            {title: "Shard", data: "shard", type: "num"},
            {title: "Type", data: "type"},
            {title: "Status", data: "status"},
            {title: "Number of Records Backfilled", data: "numRecords", type: "num"},
            {title: "Number of Affected Days", data: "numAffectedDays", type: "num"},
            {
                title: "Action",
                mData: null,
                bSortable: false,
                mRender: function (data, type, row) {
                    var table = row['table']
                    var shard = row['shard']
                    return $("<div />").append($(
                        "<button class='ui-button' onclick=\"submitBackfillJob('" + table + "'," + shard + ")\">Backfill</button>")).html();
                },
            },
            {
                title: "Last Error",
                data: "lastError",
                type: "string",
                render: function (data) {
                    return JSON.stringify(data)
                }
            },
            {
                title: "Last Start Time",
                data: "lastStartTime",
                type: "date",
                render: function (data) {
                    return new Date(data).toLocaleString()
                }
            },
            {title: "Last Duration", data: "lastDuration", type: "string"},
            {title: "Last Lock Wait Duration", data: "lockDuration", type: "string"},
            {title: "Redo Log File", data: "redologFile", type: "number"},
            {title: "Batch Offset", data: "batchOffset", type: "number"},
            {title: "Number of Records in Queue", data: "numRecords", type: "number"},
            {title: "Current Buffer Size", data: "currentBufferSize", type: "number"},
            {title: "Backfilling Buffer Size", data: "backfillingBufferSize", type: "number"},
            {title: "Max Buffer Size", data: "maxBufferSize", type: "number"},
            {title: "Num Upsert Batches", data: "numUpsertBatches", type: "number"},
        ],
        aaData: data,
    });
}
