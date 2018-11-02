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

var upsertbatchTable

function initRedoLogsPage() {
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
        refreshRedoLogFilesList()
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
        refreshRedoLogFilesList()
    });
}

function refreshRedoLogFilesList() {
    var table = $("#table-selector").select2('data')[0].text
    var shard = $("#shard-selector").select2('data')[0].text

    // Need to explicitly destroy redo log selector.
    if ($('#redologs-selector').select2()) {
        $('#redologs-selector').empty()
        $('#redologs-selector').select2("destroy");
    }

    $('#redologs-selector').select2({
        ajax: {
            url: "/dbg/{0}/{1}/redologs".format(table, shard),
            cache: true,
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
            },
        },
        width: '100px',
        minimumResultsForSearch: -1,
    }).on('change', function (e) {
        refreshUpsertBatchList()
    });
}


function refreshUpsertBatchList() {
    var table = $("#table-selector").select2('data')[0].text
    var shard = $("#shard-selector").select2('data')[0].text
    var redoLog = $("#redologs-selector").select2('data')[0].text

    // Need to explicitly destroy upsert batch selector.
    if ($('#upsertbatch-selector').select2()) {
        $('#upsertbatch-selector').empty()
        $('#upsertbatch-selector').select2("destroy");
    }

    $('#upsertbatch-selector').select2({
        ajax: {
            url: "/dbg/{0}/{1}/redologs/{2}/upsertbatches".format(table, shard, redoLog),
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
        minimumResultsForSearch: -1
    }).on('change', function () {
        initUpsertBatchTable()
    });
}

function initUpsertBatchTable() {
    var table = $("#table-selector").select2('data')[0].text
    var shard = $("#shard-selector").select2('data')[0].text
    var redoLog = $("#redologs-selector").select2('data')[0].text
    var upsertBatch = $("#upsertbatch-selector").select2('data')[0].text

    $.ajax(
        {
            url: "/dbg/{0}/{1}/redologs/{2}/upsertbatches/{3}".format(table, shard, redoLog, upsertBatch),
            success: function (body) {
                var columns = body.columnNames.map(function (name) {
                        return {"title": name}
                    }
                )

                // Need to explicitly destroy old data table.
                if (upsertbatchTable) {
                    upsertbatchTable.destroy()
                    $('#upsertbatch-table').empty()
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
                        "url": "/dbg/{0}/{1}/redologs/{2}/upsertbatches/{3}".format(table, shard, redoLog, upsertBatch),
                        "dataType": "json",
                        "contentType": 'application/json'
                    }
                });
            },
            error: function (error) {
                alert('error: ' + eval(error));
            }
        }
    )

}
