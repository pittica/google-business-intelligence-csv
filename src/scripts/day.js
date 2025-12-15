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
  getStorage,
  copiedFile,
  getFiles,
} = require("@pittica/google-cloud-storage-helpers")
const {
  getSqlFilePath,
  createFilename,
  getTemporaryTableSuffix,
  getTemporaryTableName,
  mapStorageResponse,
  getSafeFilenameFromBucket,
  mergeFiledata,
  getNow,
  formatDate,
} = require("@pittica/google-business-intelligence-helpers")
const log = require("@pittica/logger-helpers")
const { getJobMetadata } = require("../helpers/bigquery")
const { sort } = require("../helpers/storage")

/**
 * Processes all CSV files in given buckets bucket for the given date.
 *
 * @param {Date} date Element date.
 * @param {string} source Source bucket.
 * @param {string} temporary Temporary bucket.
 * @param {string} destination Destination bucket.
 */
exports.day = async (date, source, temporary, destination) => {
  if (date) {
    const day = formatDate(date)
    const now = getNow()

    log.info(`Starting "${day}" day import...`)

    this.run(day, now, source, temporary, destination)
  }
}

/**
 * Processes all CSV files in given buckets bucket for the given date.
 *
 * @param {string} day Day in YYYY-MM-DD format.
 * @param {string} now Current date in YYYY-MM-DD format.
 * @param {string} source Source bucket.
 * @param {string} temporary Temporary bucket.
 * @param {string} destination Destination bucket.
 */
exports.run = async (day, now, source, temporary, destination) => {
  const storage = getStorage()
  const dataset = await getDataset(config.dataset.temporary.name, {
    location: config.dataset.temporary.location,
  })
  const bucketSource = await storage.bucket(source)
  const bucketTemporary = await storage.bucket(temporary)
  const bucketDestination = await storage.bucket(destination)

  getFiles(storage, source, `${day}-`).then((response) =>
    this.process(
      dataset,
      bucketSource,
      bucketTemporary,
      bucketDestination,
      sort(mapStorageResponse(response, config.files.sql)),
      now
    )
  )
}

/**
 * Processes an array of files.
 *
 * @param {Dataset} dataset Dataset.
 * @param {Bucket} source Source bucket.
 * @param {Bucket} temporary Temporary bucket.
 * @param {Bucket} destination Destination bucket.
 * @param {Array} files Array of files IDs.
 * @param {string} now Current date in YYYY-MM-DD format.
 * @param {number} index Files index.
 */
exports.process = (
  dataset,
  source,
  temporary,
  destination,
  files,
  now,
  index = 0
) => {
  if (index >= 0 && index < files.length) {
    const filedata = files[index]
    const file = source.file(createFilename(filedata))

    log.info(`Importing "${file.name}"...`)

    file
      .copy(temporary.file(file.name))
      .then((temporaryResponse) => {
        const temp = copiedFile(temporaryResponse)

        if (temp) {
          deleteFile(file)

          const table = dataset.table(
            getTemporaryTableName(filedata, config.dataset.temporary.prefix)
          )

          table
            .load(temp, getJobMetadata(filedata.name))
            .then((response) => {
              if (jobDone(response)) {
                getSafeFilenameFromBucket(
                  destination,
                  mergeFiledata(filedata),
                  now
                ).then((merged) =>
                  temp
                    .copy(destination.file(`${now}/${mergeFiledata(merged)}`))
                    .then((destinationResponse) => {
                      const copy = copiedFile(destinationResponse)

                      if (copy) {
                        deleteFile(temp)

                        executeSqlFile(
                          getSqlFilePath(filedata.name, config.files),
                          dataset.bigQuery,
                          {
                            table_suffix:
                              filedata.version +
                              "-" +
                              getTemporaryTableSuffix(filedata.date),
                          }
                        )
                          .then((result) => {
                            if (result) {
                              log.success(`"${filedata.fullname}" imported...`)

                              table
                                .delete()
                                .then(() =>
                                  this.process(
                                    dataset,
                                    source,
                                    temporary,
                                    destination,
                                    files,
                                    now,
                                    index + 1
                                  )
                                )
                                .catch(logErrors)
                            }
                          })
                          .catch(logErrors)
                      }
                    })
                )
              }
            })
            .catch(logErrors)
        }
      })
      .catch(logErrors)
  }
}
