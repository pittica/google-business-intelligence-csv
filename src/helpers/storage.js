// Copyright 2024-2025 Pittica S.r.l.
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

/**
 * Sorts the given files.
 *
 * @param {Array} files An array of file data.
 * @returns The sorted files.
 */
exports.sort = (files) =>
  files.sort((a, b) => {
    const first = config.order.indexOf(a.name)
    const second = config.order.indexOf(b.name)

    if (first < second) {
      return -1
    } else if (first > second) {
      return 1
    }

    if (a.date < b.date) {
      return -1
    } else if (a.date > b.date) {
      return 1
    }

    if (a.version < a.version) {
      return -1
    } else if (b.version > b.version) {
      return 1
    }

    return 0
  })
