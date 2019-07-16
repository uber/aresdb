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

function resetToggleButton() {
    // Init health togggle.
    $.ajax({
        url: "/dbg/health",
        success: function (body) {
            $('#health-toggle').bootstrapToggle(body);
        },
        error: function (xhr) {
            alert(xhr.responseText);
        },
        async: true
    });
}

function getSchema(table, callback) {
    $.ajax({
        url: '/schema/tables/{0}'.format(table),
        success: callback,
        err: function (xhr) {
            alert(xhr.responseText);
        }
    });
}

function renderShardSelection(shards) {
    $('#shard-selector').empty();
    $('#shard-selector').select2({
        data: shards,
        width: 'resolve'
    }).on('change', function (e) {
        refreshShardViewer();
    });
    refreshShardViewer();
}

function initSummaryViewer() {
    resetToggleButton();
    $('#health-toggle').change(function () {
        var status = $(this).prop('checked') ? "on" : "off";
        $.ajax({
            url: "/dbg/health/{0}".format(status),
            success: function () {
            },
            error: function (xhr) {
                alert(xhr.responseText);
            },
            method: "POST"
        });
    });

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
                        };
                    })
                };
            }
        },
        width: 'resolve'
    }).on('change', function () {
        var table = $("#table-selector").select2('data')[0].text;
        getSchema(table, function (schema) {
            refreshShardSelection(schema.isFactTable);
        });
    });
}

function refreshShardSelection(isFactTable) {
    if (isFactTable) {
        $.ajax({
            url: "/dbg/shards",
            success: function (body) {
                renderShardSelection(body);
            },
            error: function (xhr) {
                alert(xhr.responseText);
            },
            async: true
        })
    } else {
        renderShardSelection([0]);
    }
}

function refreshShardViewer() {
    var table = $("#table-selector").select2('data')[0].text;
    var shard = $("#shard-selector").select2('data')[0].text;
    console.log(table + " , " + shard);

    // Get shard info.
    $.ajax({
        url: "/dbg/{0}/{1}".format(table, shard),
        success: function (body) {
            $("#shard-json-renderer").jsonViewer(body);
        },
        error: function (xhr) {
            alert(xhr.responseText);
        },
        async: true
    });
}