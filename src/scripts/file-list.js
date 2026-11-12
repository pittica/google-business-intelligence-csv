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

const {
  listFiles,
  txtContentToArray,
  getFileCaseInsensitive,
} = require("@pittica/google-cloud-storage-helpers")
const {
  safeFilenamesProcess,
  getFormattedDate,
} = require("@pittica/google-business-intelligence-helpers")
const { warn } = require("@pittica/logger-helpers")

/**
 * Lists missing files in the specified date.
 *
 * @param {Date|string} date Date to scan.
 * @returns {Promise<string[]>} List of missing files.
 * @since 1.6.3
 */
exports.fileList = async (date = null) => {
  const day = getFormattedDate(date, config?.date?.format || "YYYY-MM-DD")
  const match = await getFileCaseInsensitive(
    config?.bucket?.upload,
    `-${config?.files?.list?.file || "filelist"}.${config?.files?.list?.extension.toLowerCase() || "txt"}`,
    day,
    config?.projectId
  )
    .then(
      async (file) =>
        await txtContentToArray(file, config?.bucket?.upload, config?.projectId)
          .then((files) =>
            safeFilenamesProcess(files, config?.files?.safe || [])
          )
          .catch(() => [])
    )
    .catch(() => [])
  const files = await listFiles(config?.bucket?.upload, `${day}-`)
    .then((files) => safeFilenamesProcess(files, config?.files?.safe || []))

    .catch(() => [])

  const missing = match.filter((file) => !files.includes(file))

  if (missing.length > 1) {
    warn([
      `Missing files: ${missing.map((file) => `"${file}"`).join(", ")}.`,
      `Day: ${day}.`,
    ])
  }

  return missing
}
