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

package common

// JobType now we only have archiving job type.
type JobType string

const (
	// ArchivingJobType is the archiving job type.
	ArchivingJobType JobType = "archiving"
	// BackfillJobType is the backfill job type.
	BackfillJobType JobType = "backfill"
	// SnapshotJobType is the snapshot job type.
	SnapshotJobType JobType = "snapshot"
	// PurgeJobType is the purge job type.
	PurgeJobType JobType = "purge"
)
