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

const { bulk } = require("./bulk")

/**
 * Handles a Pub/Sub message event.
 *
 * @param {object} message Pub/Sub event data.
 */
exports.pubSub = async ({
  data: {
    message: { data },
  },
}) => {
  if (data && Buffer.from(data, "base64").toString() == config.pubSub.message) {
    await bulk(
      config.bucket.upload,
      config.bucket.temporary,
      config.bucket.archive
    )
  }
}
