// Copyright 2024-2026 Pittica S.r.l.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const { jobMetadata } = require("@pittica/google-bigquery-helpers")
const { getSchema } = require("@pittica/google-business-intelligence-helpers")

/**
 * Gets BigQuery load job default metadata.
 *
 * @param {string} filename Filename.
 * @returns {JobLoadMetadata} BigQuery base metadata.
 */
exports.getJobMetadata = (filename) =>
  jobMetadata(getSchema(filename, config.files.json))
