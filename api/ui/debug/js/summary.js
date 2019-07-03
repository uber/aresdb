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
        refreshShardViewer();
    });

    // Init shard selector.
    $('#shard-selector').select2({
        ajax: {
            url: "/shards",
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
    }).on('change', function (e) {
        refreshShardViewer();
    });
}

function refreshShardViewer() {
    var table = $("#table-selector").select2('data')[0].text;
    var shard = $("#shard-selector").select2('data')[0].text;

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