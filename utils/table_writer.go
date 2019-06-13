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

package utils

import (
	"bytes"
	"fmt"
)

// TableDataSource defines the interface a data source need to implement so that we can render
// a tabular representation from the data source. We get number of columns from the capacity of
// column header. the data source itself should ensure that for each get value call with row and
// col within [0,numRows) and [0, numCols), it should return valid value and should not panic.
type TableDataSource interface {
	NumRows() int
	GetValue(row, col int) interface{}
	ColumnHeaders() []string
}

func getFormatModifier(value interface{}) string {
	switch value.(type) {
	case string:
		return "s"
	case float32, float64:
		return ".2f"
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
		return "d"
	default:
		return "v"
	}
}

func expandColumnWidth(columnWidths []int, value interface{}, idx int) {
	valueWidth := len(fmt.Sprintf("%"+getFormatModifier(value), value))
	if valueWidth > columnWidths[idx] {
		columnWidths[idx] = valueWidth
	}
}

func sprintfStrings(format string, strs []string) string {
	values := make([]interface{}, len(strs))
	for i, v := range strs {
		values[i] = v
	}
	return fmt.Sprintf(format, values...)
}

// WriteTable renders a tabular representation from underlying data source.
// If there is no column for this data source, it will return an empty string.
// All elements of the table will be right justify (left padding). Column
// splitter is "|" for now.
func WriteTable(dataSource TableDataSource) string {
	columnHeaders := dataSource.ColumnHeaders()
	numCols := len(columnHeaders)

	// Return empty string if no columns.
	if numCols == 0 {
		return ""
	}
	// Compute column widths.
	columnWidths := make([]int, numCols)

	// Then compare with the capacity of each value.
	numRows := dataSource.NumRows()
	for c := 0; c < numCols; c++ {
		header := columnHeaders[c]
		columnWidths[c] = len(header)
		for r := 0; r < numRows; r++ {
			value := dataSource.GetValue(r, c)
			expandColumnWidth(columnWidths, value, c)
		}
	}

	// string buffer for final result.
	var buffer bytes.Buffer
	// Prepare format for header.
	headerFormat := "|"
	for _, columnWidth := range columnWidths {
		headerFormat += fmt.Sprintf("%%%ds|", columnWidth)
	}
	headerFormat += "\n"

	// Write column header.
	buffer.WriteString(sprintfStrings(headerFormat, columnHeaders))

	if numRows > 0 {
		// Prepare format for rows.
		rowFormat := "|"
		for c := 0; c < numCols; c++ {
			// get formatter of first row.
			value := dataSource.GetValue(0, c)
			modifier := getFormatModifier(value)
			rowFormat += fmt.Sprintf("%%%d%s|", columnWidths[c], modifier)
		}
		rowFormat += "\n"

		// Write rows.
		for r := 0; r < numRows; r++ {
			row := make([]interface{}, numCols)
			for c := 0; c < numCols; c++ {
				row[c] = dataSource.GetValue(r, c)
			}
			buffer.WriteString(fmt.Sprintf(rowFormat, row...))
		}
	}
	return buffer.String()
}
