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

const {
  getDataset,
  executeSqlFile,
  jobDone,
  logErrors,
} = require("@pittica/google-bigquery-helpers")
const {
  deleteFile,
  copyFile,
  copiedFile,
  getStorage,
} = require("@pittica/google-cloud-storage-helpers")
const {
  getSqlFilePath,
  splitName,
  getTemporaryTableSuffix,
  getTemporaryTableName,
  isDataCsv,
} = require("@pittica/google-business-intelligence-helpers")
const log = require("@pittica/logger-helpers")
const { getJobMetadata } = require("../helpers/bigquery")

/**
 * Processes a file from the given buckets.
 *
 * @param {string} name Filename.
 * @param {string} bucketSource Source bucket.
 * @param {string} bucketTemporary Temporary bucket.
 * @param {string} bucketDestination Destination bucket.
 * @param {boolean} clean A value indicating whether the temporary table will be deleted.
 * @returns {boolean} A value indicating whether the process has been done.
 */
exports.file = async (
  name,
  bucketSource,
  bucketTemporary,
  bucketDestination,
  clean = true
) => {
  const filedata = splitName(name)

  if (isDataCsv(filedata, config.files.json)) {
    const storage = getStorage()
    const source = storage.bucket(bucketSource).file(name)
    const [exists] = await source.exists()

    if (exists) {
      return await copyFile(source, bucketTemporary)
        .then(async (response) => {
          const file = copiedFile(response)

          if (file !== null) {
            deleteFile(source)

            const ds = await getDataset(config.dataset.temporary.name, {
              location: config.dataset.temporary.location,
            })
            const day = getNow()
            const table = await ds.table(
              getTemporaryTableName(filedata, config.dataset.temporary.prefix)
            )

            return await this.run(
              table,
              bucketDestination,
              file,
              filedata,
              day,
              clean
            )
          }

          return false
        })
        .catch(logErrors)
    }
  }

  log.error([404, name])

  return false
}

/**
 * Runs a jobs and imports a CSV file.
 *
 * @param {Table} table Table object.
 * @param {string} destination Google Cloud Storage destination bucket.
 * @param {File} file Google Cloud Storage temporary bucket.
 * @param {object} filedata File data.
 * @param {string} day Day in the YYYY-MM-DD format.
 * @param {boolean} clean A value indicating whether the temporary table will be deleted.
 * @returns {boolean} A value indicating whether the process has been done.
 */
exports.run = async (
  table,
  destination,
  file,
  filedata,
  day,
  clean = false
) => {
  const storage = getStorage()

  return await table
    .load(file, getJobMetadata(filedata.name))
    .then(async (response) => {
      if (jobDone(response)) {
        return await file
          .copy(storage.bucket(destination).file(`${day}/${file.name}`))
          .then(async () => {
            deleteFile(file)

            return await executeSqlFile(
              getSqlFilePath(filedata.name, config.files),
              table.bigQuery,
              {
                table_suffix:
                  filedata.version +
                  "-" +
                  getTemporaryTableSuffix(filedata.date),
              }
            )
              .then(async (result) =>
                result
                  ? clean
                    ? await table
                        .delete()
                        .then(() => true)
                        .catch(logErrors)
                    : true
                  : false
              )
              .catch(logErrors)
          })
          .catch(logErrors)
      }

      log.info(response)

      return false
    })
}
