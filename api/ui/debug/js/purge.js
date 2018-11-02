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

function initSchedulerViewer() {
    // Disable null value warning from data table.
    $.fn.dataTable.ext.errMode = 'none';

    $.ajax({
            url: "/dbg/jobs/purge",
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

                    if (row['status'] == 'running') {
                        runningJobData.push(row);
                    } else {
                        pastRunsData.push(row);
                    }
                }
                initRunningJobTable(runningJobData);
                initPastRunTable(pastRunsData);
                initSafePurgeButton();
            },
            error: function (xhr) {
                alert(xhr.responseText);
            }
        }
    );
    initDatePickModal();

}

function initSafePurgeButton() {
    $('#purge-it').click(function() {
        const button = $(this);
        const table = button.data('table');
        const shard = button.data('shard');
        submitPurgeJob(table, shard, 0, 0, true);
    });
}

function initDatePickModal() {
    $('#start-batch-pick').datepicker();
    $('#end-batch-pick').datepicker();
    $('#date-pick-modal').on('show.bs.modal', function (event) {
        var button = $(event.relatedTarget);
        var table = button.data('table');
        var shard = button.data('shard');
        var modal = $(this);
        modal.find('#purge-table-name').val(table);
        modal.find('#purge-shard-id').val(shard)
    });

    $('#purge-button').click(function () {
        var startDate = $('#start-batch-pick').val();
        var endDate = $('#end-batch-pick').val();
        var table = $('#purge-table-name').val();
        var shard = $('#purge-shard-id').val();
        var batchIDStart = 0;
        var batchIDEnd = 0;
        if (startDate) {
            batchIDStart = Math.floor(new Date(startDate).getTime() / 1000 / 86400);
        }
        if (endDate) {
            batchIDEnd = Math.floor(new Date(endDate).getTime() / 1000 / 86400);
        }

        if (batchIDEnd <= batchIDStart) {
            alert("invalid batch start and end: [" + batchIDStart + "," + batchIDEnd + ")");
        }

        $.ajax({
            url: "/schema/tables/{0}".format(table),
            method: 'GET',
            success: function (schema) {
                var retentionDays = schema.config.recordRetentionInDays;
                if (retentionDays > 0) {
                    var now = Math.floor(Date.now() / 1000 / 86400);
                    if (batchIDEnd >= (now - retentionDays)) {
                        if (!confirm('Are you sure to purge data within retention from ' + (now-retentionDays) + ' to ' + batchIDEnd)) {
                            return;
                        }
                    }
                }
                submitPurgeJob(table, shard, batchIDStart, batchIDEnd, false);
            },
            error: function (xhr) {
                alert(xhr.responseText);
            }
        });
    });
}

function submitPurgeJob(table, shard, batchIDStart, batchIDEnd, safePurge) {
    $.ajax({
        url: "/dbg/{0}/{1}/purge".format(table, shard),
        method: "POST",
        dataType: 'json',
        data: {
            batchIDStart: batchIDStart,
            batchIDEnd: batchIDEnd,
            safePurge: safePurge,
        },
        success: function (body) {
            alert(body);
            reloadCurrentTab();
        },
        error: function (xhr) {
            alert(xhr.responseText);
        }
    })
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
            {title: "Current", data: "current"},
            {title: "Total", data: "total"}
        ],
        aaData: data
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
            {title: "Number of Batches Purged", data: "numBatches"},
            {title: "Batch Start", data: "batchIDStart"},
            {title: "Batch End", data: "batchIDEnd"},
            {
                title: "Action",
                mData: null,
                bSortable: false,
                mRender: function (data, type, row) {
                    var table = row['table'];
                    var shard = row['shard'];
                    return $("<div />").append($(
                        "<button class='ui-button' id='purge-it' data-table='" + table + "' data-shard='" + shard + "'>safe purge</button>")).append($(
                        "<button class='ui-button' data-toggle='modal' data-target='#date-pick-modal' data-table='" + table + "' data-shard='" + shard + "'>customized purge</button>")
                    ).html();
                }
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
        ],
        aaData: data,
    });
}
