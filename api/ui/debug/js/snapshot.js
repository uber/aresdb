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
            url: "/dbg/jobs/snapshot",
            success: function (body) {
                var runningJobData = [];
                var pastRunsData = [];
                for (var key in body) {
                    var strs = key.split("|");
                    var row = body[key];
                    row['table'] = strs[0];
                    row['shard'] = strs[1];
                    row['type'] = strs[2];
                    if ('lastDuration' in row) {
                        row['lastDuration'] = row['lastDuration'].toDuration();
                    }

                    if ('lockDuration' in row) {
                        row['lockDuration'] = row['lockDuration'].toDuration();
                    }

                    if (row['status'] == 'running') {
                        runningJobData.push(row);
                    } else {
                        pastRunsData.push(row);
                    }
                }
                initRunningJobTable(runningJobData);
                initPastRunTable(pastRunsData);
            },
            error: function (xhr) {
                alert(xhr.responseText);
            }
        }
    );
}

function submitsnapshotJob(table, shard) {
    var url = "/dbg/{0}/{1}/snapshot".format(table, shard)
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
            {title: "Number of Mutations since Last Snapshot", data: "numMutations", type: "num"},
            {
                title: "Action",
                mData: null,
                bSortable: false,
                mRender: function (data, type, row) {
                    var table = row['table']
                    var shard = row['shard']
                    return $("<div />").append($(
                        "<button class='ui-button' onclick=\"submitsnapshotJob('" + table + "'," + shard + ")\">snapshot</button>")).html();
                },
            },
            {
                title: "Last Error",
                data: "lastError",
                type: "string",
                render: function (data) {
                    return JSON.stringify(data);
                }
            },
            {
                title: "Last Start Time",
                data: "lastStartTime",
                type: "date",
                render: function (data) {
                    return new Date(data).toLocaleString();
                }
            },
            {title: "Last Duration", data: "lastDuration", type: "string"},
            {title: "Redo Log File", data: "redologFile", type: "number"},
            {title: "Batch Offset", data: "batchOffset", type: "number"},
        ],
        aaData: data,
    });
}
