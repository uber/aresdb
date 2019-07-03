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

if (!String.prototype.format) {
    String.prototype.format = function () {
        var args = [].slice.call(arguments);
        return this.replace(/(\{\d+\})/g, function (a) {
            return args[+(a.substr(1, a.length - 2)) || 0];
        });
    };
}

const maxRowsPerPage = 100;
const pagesToShow = 10;
const maxValueLength = 100;
var currentTableName = "";
var currentShardID = 0;
var currentBatchID = 0;

function Iterator(column, numRows) {
    this.values = column.values;
    this.counts = column.counts;
    if (!this.values || !this.counts.length || !this.counts || !this.counts.length) {
        this.values = [null];
        this.counts = [numRows];
    }
    this.currentRow = 0;
    this.currentIndex = 0;
}

Iterator.prototype.shouldSkip = function () {
    return this.currentRow !== 0 && this.currentRow !== this.counts[this.currentIndex - 1];
};

Iterator.prototype.advance = function () {
    this.currentRow++;
    if (this.currentRow === this.counts[this.currentIndex]) {
        this.currentIndex++;
    }
};

Iterator.prototype.read = function () {
    return {
        value: this.values[this.currentIndex] === 'undefined' ? 'NULL' : this.values[this.currentIndex],
        count: this.currentIndex === 0 ? this.counts[this.currentIndex] : this.counts[this.currentIndex] - this.counts[this.currentIndex - 1]
    }
};

function renderTables(tables) {
    var tableSelect = $('#table-select');
    tables.forEach(function (table, id) {
        tableSelect.append('<option value' + '=' + id + '>' + table + '</option>');
    });
}

function renderShards(shards) {
    var shardSelect = $('#shard-select');
    shards.forEach(function (shard, id) {
        shardSelect.append('<option value' + '=' + id + '>' + shard + '</option>');
    });
}

function getBatchSize(shard, id) {
    var batch = shard.liveStore.batches[id];
    if (shard.liveStore.lastReadRecord.batchID == id) {
        return shard.liveStore.lastReadRecord.index;
    }
    return batch.capacity;
}

function renderShard(shard) {
    var batchTableBody = $('#batch-table-body');
    batchTableBody.empty();
    Object.keys(shard.liveStore.batches).forEach(function (id) {
        var batchSize = getBatchSize(shard, id);
        batchTableBody.append('<tr id="batch-tr-{0}" class="clickable" onclick="loadBatchTable({1}, {2})"><td>{3}</td><td>{4}</td></tr>'.format(id, id, batchSize, id, batchSize));
    });

    Object.keys(shard.archiveStore.currentVersion.batches).forEach(function (id) {
        var batch = shard.archiveStore.currentVersion.batches[id];
        var date = new Date(parseInt(id, 10) * 86400000)
        var idStr = '{0} {1}-{2}-{3}'.format(id, date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
        batchTableBody.append('<tr id="batch-tr-{0}" class="clickable" onclick="loadBatchTable({1}, {2})"><td nowrap>{3}</td><td>{4}</td></tr>'.format(id, id, batch.size, idStr, batch.size));
    });

    var numLiveRecords = shard.liveStore.lastReadRecord.index + shard.liveStore.batchSize * Object.keys(shard.liveStore.batches).filter(function (id) {
        return id != shard.liveStore.lastReadRecord.batchID;
    }).length;

    var primaryKeyView = $('#primary-key-view');
    primaryKeyView.empty();
    var primaryKeyStats = $('<table></table>');
    primaryKeyStats.append('<tr><td><b>Primary Key: </b></td></tr>');
    primaryKeyStats.append('<tr><td>' +
        ' <label for="primary-key-lookup-input">Primary Key Lookup: </label></td><td><input class="small-input" id="primary-key-lookup-input" type="text"/></td></tr>')
    primaryKeyStats.append('<tr><td>Allocated Memory (MB):</td><td>{0}</td></tr>'.format(shard.liveStore.primaryKey.allocatedBytes / 1024 / 1024));
    primaryKeyStats.append('<tr><td>Percentage:</td><td>{0}% ({1} / {2})</td></tr>'.format(numLiveRecords * 100 / shard.liveStore.primaryKey.capacity, numLiveRecords, shard.liveStore.primaryKey.capacity));
    primaryKeyStats.append('<tr><td>Cutoff:</td><td>{0}</td></tr>'.format(new Date(shard.liveStore.primaryKey.eventTimeCutoff * 1000).toLocaleString()));
    primaryKeyView.append(primaryKeyStats);

    var pkLookupInput = $('#primary-key-lookup-input')
    pkLookupInput.keyup(function (e) {
        if (e.keyCode === 13) {
            var uuid = $('#primary-key-lookup-input').val();
            $.ajax({
                url: '/dbg/{0}/{1}/primary-keys?key={2}'.format(currentTableName, currentShardID, uuid),
                dataType: 'json',
                success: function (recordID) {
                    var batchID = recordID.batchID;
                    var row = recordID.index;
                    var batchSize = getBatchSize(shard, batchID);
                    jumpToRowOfBatch(row, batchID, batchSize, true);
                },
                error: function (xhr) {
                    alert(xhr.responseText);
                }
            });
        }
    });
}

function getPolygonUrl(polygonStr) {
    var pointStrs = polygonStr.replace("Polygon", "").split(",");
    var sumLat = 0;
    var sumLong = 0;
    var path = "";
    for (var i in pointStrs) {
        var pointStr = pointStrs[i];
        var point = pointStr.replace(/[\(\)]/g, "").split("+");
        var lat = parseFloat(point[1]);
        var long = parseFloat(point[0]);
        path += '|{0},{1}'.format(lat, long);
        sumLat += lat;
        sumLong += long;
    }
    var avgLat = sumLat / pointStrs.length;
    var avgLong = sumLong / pointStrs.length;
    return 'http://maps.googleapis.com/maps/api/staticmap?center={0},{1}&size=2000x2000&zoom=10&sensor=false&path=color:0xff0000ff|weight:5{2}'.format(avgLat, avgLong, path);
}

function renderBatch(highlightRowNumber) {
    return function (json) {
        var header = document.getElementById('table-header');
        var html = '<th>Row Number</th>';
        for (var i = 0; i < json.columns.length; i++) {
            if (json.deleted !== null && json.deleted.includes(i)) {
                html += '<th class="deleted-column data-column">' + json.columns[i] + '<br>' + json.types[i] + '</th>';
            } else {
                html += '<th class="data-column">' + json.columns[i] + '<br>' + json.types[i] + '</th>';
            }
        }
        header.innerHTML = html;

        var iterators = json.columns.map(function (columnName, id) {
            if (id < json.vectors.length) {
                return new Iterator(json.vectors[id], json.numRows)
                    ;
            } else {
                return new Iterator({}, json.numRows);
            }
        });

        var body = document.getElementById('table-data');
        html = '';
        for (var r = 0; r < json.numRows; r++) {
            var rowCls = "";
            if (r == highlightRowNumber) {
                rowCls = "highlight";
            }
            html += '<tr class="{0}"><td class="data-column">'.format(rowCls) + (r + json.startRow) + '</td>';
            for (var c = 0; c < iterators.length; c++) {
                if (!iterators[c].shouldSkip()) {
                    var valueCount = iterators[c].read();
                    var ts = '';
                    if (c == 0 && json.types[0] == 'Uint32') {
                        ts = '<br>';
                        ts += new Date(valueCount.value * 1000).toLocaleString();
                    }
                    var fullValue = '' + valueCount.value;
                    if (fullValue.length > maxValueLength) {
                        html += '<td class="data-column short-value-column" data-value="{0}" rowspan="{1}">{2}</td>'.format(fullValue, valueCount.count, fullValue.substr(0, maxValueLength) + ts);
                    } else {
                        html += '<td class="data-column" rowspan="{0}">{1}</td>'.format(valueCount.count, fullValue + ts);
                    }
                }
                iterators[c].advance();
            }
            html += '</tr>';
        }
        body.innerHTML = html;

        // add popup for shortened value
        var shortColumns = document.getElementsByClassName("short-value-column");
        for (var j = 0; j < shortColumns.length; j++) {
            shortColumns[j].addEventListener("click", function () {
                var fullValue = (this).getAttribute("data-value");
                if (fullValue.startsWith("Polygon")) {
                    var url = getPolygonUrl(fullValue);
                    var win = window.open(url, '_blank');
                    win.focus();
                } else {
                    alert(fullValue);
                }
            });
        }

        // populate column select
        renderColumns(json.columns);
    };
}

function jumpToRowOfBatch(rowNumber, batchID, batchSize, highlight) {
    var prevBatchID = currentBatchID
    currentBatchID = batchID;
    if (currentBatchID != prevBatchID) {
        var prevElem = $("#batch-tr-{0}".format(prevBatchID))
        if (prevElem.length && prevElem.hasClass("highlight")) {
            prevElem.removeClass("highlight");
        }

        var curElem = $("#batch-tr-{0}".format(currentBatchID))
        if (curElem.length && !curElem.hasClass("highlight")) {
            curElem.addClass("highlight");
        }
    }
    var maxPage = Math.floor(batchSize / maxRowsPerPage) + 1;
    var startPage = Math.max(0, Math.floor(rowNumber / maxRowsPerPage));
    setPages(startPage, startPage + pagesToShow, maxPage);

    var highlightRowNumber = rowNumber % maxRowsPerPage;
    if (!highlight) {
        highlightRowNumber = -1;
    }

    loadBatch(startPage * maxRowsPerPage, maxRowsPerPage, highlightRowNumber);
}

function loadBatchTable(batchID, batchSize) {
    $('#row-jump').show();
    var rowNumberInput = $('#row-number-input');
    rowNumberInput.focus();
    rowNumberInput.keyup(function (e) {
        if (e.keyCode === 13) {
            var rowNumber = $('#row-number-input').val();
            jumpToRowOfBatch(rowNumber, batchID, batchSize, false);
        }
    });

    jumpToRowOfBatch(0, batchID, batchSize, false);
}

function setPages(startPage, endPage, maxPage) {
    var pagination = $('#batch-pagination');
    pagination.empty();
    pagination.append('<a href="#" onclick="setPages({0},{1},{2})">&laquo;</a>'.format(Math.max(startPage - pagesToShow, 0), Math.max(pagesToShow, startPage), maxPage));
    for (var i = startPage; i < Math.min(endPage, maxPage); i++) {
        pagination.append('<a href="#" onclick="loadBatch({0},{1},{2})">{3}</a>'.format(i * maxRowsPerPage, maxRowsPerPage, -1, i + 1));
    }
    pagination.append('<a href="#" onclick="setPages({0},{1},{2})">&raquo;</a>'.format(endPage, endPage + pagesToShow, maxPage));
}

function renderColumns(columns) {
    var columnSelect = $('#column-select');
    columnSelect.empty();
    columns.forEach(function (column, id) {
        columnSelect.append('<option value' + '=' + id + '>' + column + '</option>');
    });
}

function loadBatch(startRow, numRows, highlightRowNumber) {
    $.getJSON(
        '/dbg/{0}/{1}/batches/{2}?startRow={3}&numRows={4}'.format(currentTableName, currentShardID, currentBatchID, startRow, numRows),
        {},
        renderBatch(highlightRowNumber)
    );
}

function listBatches() {
    $.getJSON(
        '/dbg/{0}/{1}'.format(currentTableName, currentShardID),
        {},
        renderShard
    );
}

function listTables() {
    $.getJSON(
        '/schema/tables',
        {},
        renderTables
    );
}

function listShards() {
    $.getJSON(
        '/dbg/shards',
        {},
        renderShards
    );
}

function getTableSchema(table) {
    $.getJSON(
        '/schema/tables/{0}'.format(table),
        {},
        function (schema) {
            var columns = schema.columns.map(function (column) {
                return column.name;
            });
            renderColumns(columns);
        }
    );
}

function loadVectorParty(batchID, columnName) {
    $.ajax({
        url: '/dbg/{0}/{1}/batches/{2}/vector-parties/{3}'.format(currentTableName, currentShardID, batchID, columnName)
    }).done(listBatches);
}

$(document).ready(function () {
    $('#row-jump').hide();
    initShardPicker();
    initBatchLoader();
    listTables();
    listShards();
});

function initShardPicker() {
    var shardPicker = $('#shard-pick');
    shardPicker.append('<label for="table-select">Table: </label>');
    shardPicker.append('<select id="table-select"></select>');
    shardPicker.append('<label for="shard-select">Shard: </label>');
    shardPicker.append('<select id="shard-select" ></select>');
    var shardPickButton = $('<button class="medium-button">Submit</button>');
    shardPickButton.click(function (event) {
        currentTableName = $('#table-select').find(":selected").text();
        currentShardID = $('#shard-select').find(":selected").text();
        listBatches();
        getTableSchema(currentTableName);
    });
    shardPicker.append(shardPickButton);
}

function initBatchLoader() {
    var batchInput = $('<input class="small-input" id="time-picker"/>').datetimepicker();
    var columnSelect = $('<select id="column-select"/>');
    var vpLoadButton = $('<button class="medium-button">Load</button>').click(function () {
        var time = batchInput.val();
        if (!time) {
            alert("No batch id specified!");
            return
        }
        var batchID = Math.floor(new Date(time).getTime() / 1000 / 86400);
        var column = $('#column-select').find(":selected").text();
        if (!column) {
            alert("no column selected!");
            return
        }
        loadVectorParty(batchID, column);
    });

    var batchLoad = $('#batch-load');
    batchLoad.append();
    batchLoad.append('<label for="time-picker">BatchID: </label>');
    batchLoad.append(batchInput);
    batchLoad.append('<label for="column-select">Column: </label>');
    batchLoad.append(columnSelect);
    batchLoad.append(vpLoadButton);
}
