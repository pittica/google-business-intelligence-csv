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

const { bulk } = require("./bulk")
const { unlistened } = require("./unlistened")
const log = require("@pittica/logger-helpers")

/**
 * Processes all CSV files in upload bucket bucket then removes not allowed files.
 */
exports.bulkUnlistened = async () => {
  await bulk().then(() => log.info("Import completed."))

  log.info("Removing all not allowed files in upload bucket...")

  await unlistened()

  log.info("All not allowed files removed in upload bucket.")
}
