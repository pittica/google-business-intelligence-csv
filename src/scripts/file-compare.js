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
  getFormattedDate,
  scripts: { comparison },
} = require("@pittica/google-business-intelligence-helpers")
const { warn } = require("@pittica/logger-helpers")

/**
 * Compares files for the specified date.
 *
 * @param {Date|string} date Date to compare files for.
 * @returns {Promise<boolean>} A value indicating whether the files are consistent or not.
 */
exports.fileCompare = async (date = null) => {
  const day = getFormattedDate(date)
  const files = await comparison(config.bucket.upload, day).catch((err) => {
    warn(err.message)
    return []
  })

  if (files.length > 0) {
    warn(
      `Missing files: ${files.map((file) => `"${file}"`).join(", ")}. Day: ${day}`
    )

    return false
  }

  return true
}
