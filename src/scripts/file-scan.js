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

const { listFiles } = require("@pittica/google-cloud-storage-helpers")
const {
  splitName,
  getFormattedDate,
} = require("@pittica/google-business-intelligence-helpers")
const { warn } = require("@pittica/logger-helpers")

/**
 * Scans for files in the specified date.
 *
 * @param {Date|string} date Date to scan.
 */
exports.fileScan = async (date = null) => {
  const day = getFormattedDate(date)
  const files = await listFiles(config.bucket.upload, `${day}-`)
    .then((response) => response.map((file) => splitName(file)))
    .catch(() => [])
  const check = config.order.filter(
    (file) =>
      config.files.safe.includes(file) &&
      !files.some(
        ({ name, extension }) =>
          name === file && extension.toUpperCase() === "CSV"
      )
  )
  if (check.length > 1) {
    warn(
      `Missing files: ${check.map((file) => `"${file}"`).join(", ")}. Day: ${day}.`
    )
  }
}
