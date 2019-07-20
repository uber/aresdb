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

// Define format on string prototype.
if (!String.prototype.format) {
    String.prototype.format = function () {
        var args = [].slice.call(arguments);
        return this.replace(/(\{\d+\})/g, function (a) {
            return args[+(a.substr(1, a.length - 2)) || 0];
        });
    };
}

// Define duration formatting on Number
if (!Number.prototype.toDuration) {
    Number.prototype.toDuration = function () {
        var sec = this / 1E9 // Nanoseconds to seconds.
        var hours = Math.floor(sec / 3600)
        var minutes = Math.floor((sec - (hours * 3600)) / 60)
        var seconds = Math.floor(sec - (hours * 3600) - (minutes * 60))

        if (hours < 10) {
            hours = "0" + hours
        }
        if (minutes < 10) {
            minutes = "0" + minutes
        }
        if (seconds < 10) {
            seconds = "0" + seconds
        }
        return hours + ':' + minutes + ':' + seconds
    }
}

function reloadCurrentTab() {
    var current_index = $("#tabs").tabs("option", "selected");
    location.reload()
    $("#tabs").tabs('load', current_index);
}

function promiseFromAjax(opts, callback) {
    if (callback) {
        return new Promise((resolve, reject) =>
            $.ajax(opts).done(x => resolve(callback(x))).fail((x, y, err) => reject(err)));
    }
    return new Promise((resolve, reject) =>
        $.ajax(opts).done(x => resolve(x)).fail((x, y, err) => reject(err)));
}


