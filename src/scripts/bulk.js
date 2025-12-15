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

const { getDataset, logErrors } = require("@pittica/google-bigquery-helpers")
const { getStorage } = require("@pittica/google-cloud-storage-helpers")
const {
  extractStorageResponse,
  getNow,
} = require("@pittica/google-business-intelligence-helpers")
const log = require("@pittica/logger-helpers")
const { sort } = require("../helpers/storage")
const { process } = require("./day")

/**
 * Processes all CSV files in given buckets.
 *
 * @param {string} source Source bucket.
 * @param {string} temporary Temporary bucket.
 * @param {string} destination Destination bucket.
 */
exports.bulk = async (source, temporary, destination) => {
  const storage = getStorage()
  const dataset = await getDataset(config.dataset.temporary.name, {
    location: config.dataset.temporary.location,
  })
  const bucketSource = await storage.bucket(source)
  const bucketTemporary = await storage.bucket(temporary)
  const bucketDestination = await storage.bucket(destination)

  const now = getNow()

  storage
    .bucket(source)
    .getFiles()
    .then((response) => {
      log.info(`Starting bulk import...`)

      const files = extractStorageResponse(response, config.files.sql)

      process(
        dataset,
        bucketSource,
        bucketTemporary,
        bucketDestination,
        sort(files),
        now
      )
    })
    .catch(logErrors)
}
