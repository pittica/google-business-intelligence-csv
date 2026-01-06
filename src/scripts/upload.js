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

const { splitName } = require("@pittica/google-business-intelligence-helpers")
const { day } = require("./day")

/**
 * Handles an "upload" event in Google Cloud Storage.
 *
 * @param {object} message Contains the event message which contains settings and type.
 * @param {string} last Last uploaded file name without extension used as trigger.
 */
exports.upload = async (
  { type, data: { bucket, name } },
  last = "till_status"
) => {
  if (
    type === "google.cloud.storage.object.v1.finalized" &&
    bucket === config.bucket.upload
  ) {
    const file = splitName(name)

    if (file.name === last) {
      await day(
        file.date,
        config.bucket.upload,
        config.bucket.temporary,
        config.bucket.archive
      )
    }
  }
}
