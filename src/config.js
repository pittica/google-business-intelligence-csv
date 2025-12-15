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

const path = require("path")

/**
 * Configuration.
 *
 * @param {object} config Configuration parameters.
 */
exports.config = (config = {}) => {
  const initial = {
    bucket: {
      upload: "upload",
      temporary: "temp",
      archive: "archive",
    },
    pubSub: {
      message: "upload",
    },
    dataset: {
      temporary: {
        name: "tmp",
        prefix: "tmp_csv_",
        location: "us-central1",
      },
    },
    files: {
      json: path.join(process.cwd(), "json"),
      sql: path.join(process.cwd(), "sql"),
    },
    order: [],
  }

  global.config = {
    ...initial,
    ...config,
  }

  return global.config
}
