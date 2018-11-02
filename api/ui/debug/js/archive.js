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
            url: "/dbg/jobs/archiving",
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
                    if (row['status'] == 'running') {
                        runningJobData.push(row)
                    } else {
                        pastRunsData.push(row)
                    }
                }
                initRunningJobTable(runningJobData)
                initPastRunTable(pastRunsData)
            },
            error: function (xhr) {
                alert(xhr.responseText)
            }
        }
    )
}

function openArchiveDialog(table, shard) {
    var timePicker = $('#time-picker')
    if (timePicker) {
        timePicker.datetimepicker('destroy')
    }
    timePicker.datetimepicker()
    $('#archiving-dialog').dialog()
    var url = "/dbg/{0}/{1}/archive".format(table, shard)
    $('#submit').on("click", function () {
        var cutoff = $("#cutoff").val()
        // if cutoff is empty , read from date picker.
        if (!cutoff) {
            var timePickerVal = $('#time-picker').val()
            if (!timePickerVal) {
                alert("Please input cutoff!")
                return
            }
            var dt = new Date(timePickerVal)
            // Get seconds.
            cutoff = dt.getTime() / 1000
            console.log(cutoff)
        }

        $.ajax({
                url: url,
                method: "POST",
                data: JSON.stringify({cutoff: parseInt(cutoff)}),
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
            {
                title: "Running Cutoff",
                data: "runningCutoff",
                render: function (data) {
                    return data + " " + new Date(data * 1000).toLocaleString()
                }
            },
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
            {
                title: "Current Cutoff",
                data: "currentCutoff",
                render: function (data) {
                    return data + " " + new Date(data * 1000).toLocaleString()
                }
            },
            {title: "Status", data: "status"},
            {
                title: "Next Run",
                data: "nextRun",
                type: "date",
                render: function (data) {
                    return new Date(data).toLocaleString()
                }
            },
            {title: "Number of Records Archived", data: "numRecords", type: "num"},
            {title: "Number of Affected Days", data: "numAffectedDays", type: "num"},
            {
                title: "Action",
                mData: null,
                bSortable: false,
                mRender: function (data, type, row) {
                    var table = row['table']
                    var shard = row['shard']
                    return $("<div />").append($(
                        "<button class='ui-button' onclick=\"openArchiveDialog('" + table + "'," + shard + ")\">Archive</button>")).html();
                },
            },
            {
                title: "Last Cutoff",
                data: "lastCutoff",
                render: function (data) {
                    return data + " " + new Date(data * 1000).toLocaleString()
                }
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
        ],
        aaData: data,
    });
}
