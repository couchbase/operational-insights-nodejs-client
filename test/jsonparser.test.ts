/*
 *  Copyright 2016-2025. Couchbase, Inc.
 *  All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { parserStream } from 'stream-json'
import { pipeline, Readable } from 'node:stream'
import { assert } from 'chai'
import { harness } from './harness.js'
import { JsonTokenParserStream } from '../lib/jsonparser.js'
import { QueryMetadata } from '../lib/querytypes.js'

describe('JsonTokenParserStream', function () {
  async function collectRowsAndErrors(jsonString: string): Promise<{
    rows: string[]
    errorsItems: string[] | null
    stack: any
  }> {
    return new Promise((resolve, reject) => {
      const rows: string[] = []
      let errorsItems: string[] | null = null
      const jsonParser = new JsonTokenParserStream()

      jsonParser.on('data', (rowJson: string) => {
        rows.push(rowJson)
      })

      jsonParser.on('errorsComplete', (itemsArray: string[]) => {
        errorsItems = itemsArray.slice()
      })

      jsonParser.on('end', () => {
        resolve({ rows, errorsItems, stack: jsonParser.stack.pop() })
      })

      pipeline(Readable.from([jsonString]), parserStream(), jsonParser, (err) => {
        if (err) {
          reject(err)
        }
      })
    })
  }

  it('should successfully parse a successful response', async function () {
    const successfulJson = `
    {
      "requestID": "94c7f89f-92b6-4aba-a90d-be715ca47309",
      "signature": { "*": "*" },
      "results": [
        { "id": 1 },
        { "id": 2 }
      ],
      "plans": {},
      "status": "success",
      "metrics": {
        "elapsedTime": "14.927542ms",
        "executionTime": "12.875792ms",
        "compileTime": "4.178042ms",
        "queueWaitTime": "0ns",
        "resultCount": 2,
        "resultSize": 2,
        "processedObjects": 2,
        "bufferCacheHitRatio": "100.00%"
      }
    }
    `
    const { rows, errorsItems, stack } =
      await collectRowsAndErrors(successfulJson)

    assert.equal(rows.length, 2)
    assert.deepEqual(rows[0], '{"id":1}')
    assert.deepEqual(rows[1], '{"id":2}')
    assert.isNull(errorsItems)

    const metadata = QueryMetadata.parse(stack.value)
    assert.deepEqual(metadata.requestId, '94c7f89f-92b6-4aba-a90d-be715ca47309')
    assert.isEmpty(metadata.warnings)
    assert.isNotNull(metadata.metrics)
    assert.equal(metadata.metrics.elapsedTime, 14.927542)
    assert.equal(metadata.metrics.executionTime, 12.875792)
    assert.equal(metadata.metrics.resultCount, 2)
    assert.equal(metadata.metrics.resultSize, 2)
    assert.equal(metadata.metrics.processedObjects, 2)
  })

  it('should successfully parse a response with nested objects in rows', async function () {
    const successfulJson = `
    {
      "requestID": "94c7f89f-92b6-4aba-a90d-be715ca47309",
      "signature": { "*": "*" },
      "results": [
        {"someCollection":{"id":"6d68232d-c3d1-4d8e-a588-0df9cb03e36e","content":"initial","created_at":"2025-06-03T11:32:56.701Z"}},
        {"someOtherCollection":{"id":"6d68232d-c3d1-4d8e-a588-0df9cb03e36f","content":"initial","created_at":"2025-06-03T11:32:58.701Z"}}
      ],
      "plans": {},
      "status": "success",
      "metrics": {
        "elapsedTime": "14.927542ms",
        "executionTime": "12.875792ms",
        "compileTime": "4.178042ms",
        "queueWaitTime": "0ns",
        "resultCount": 2,
        "resultSize": 2,
        "processedObjects": 2,
        "bufferCacheHitRatio": "100.00%"
      }
    }
    `
    const { rows, errorsItems, stack } =
      await collectRowsAndErrors(successfulJson)

    assert.equal(rows.length, 2)
    assert.deepEqual(
      rows[0],
      '{"someCollection":{"id":"6d68232d-c3d1-4d8e-a588-0df9cb03e36e","content":"initial","created_at":"2025-06-03T11:32:56.701Z"}}'
    )
    assert.deepEqual(
      rows[1],
      '{"someOtherCollection":{"id":"6d68232d-c3d1-4d8e-a588-0df9cb03e36f","content":"initial","created_at":"2025-06-03T11:32:58.701Z"}}'
    )
    assert.isNull(errorsItems)

    const metadata = QueryMetadata.parse(stack.value)
    assert.deepEqual(metadata.requestId, '94c7f89f-92b6-4aba-a90d-be715ca47309')
    assert.isEmpty(metadata.warnings)
    assert.isNotNull(metadata.metrics)
    assert.equal(metadata.metrics.elapsedTime, 14.927542)
    assert.equal(metadata.metrics.executionTime, 12.875792)
    assert.equal(metadata.metrics.resultCount, 2)
    assert.equal(metadata.metrics.resultSize, 2)
    assert.equal(metadata.metrics.processedObjects, 2)
  })

  it('should successfully parse a response with primitives in rows', async function () {
    const successfulJson = `
    {
      "requestID": "94c7f89f-92b6-4aba-a90d-be715ca47309",
      "signature": { "*": "*" },
      "results": [
        true,
        false,
        { "id": 1 },
        {"someCollection":{"id":"6d68232d-c3d1-4d8e-a588-0df9cb03e36e","content":"initial","created_at":"2025-06-03T11:32:56.701Z"}},
        {},
        null,
        "string"
      ],
      "plans": {},
      "status": "success",
      "metrics": {
        "elapsedTime": "14.927542ms",
        "executionTime": "12.875792ms",
        "compileTime": "4.178042ms",
        "queueWaitTime": "0ns",
        "resultCount": 2,
        "resultSize": 2,
        "processedObjects": 2,
        "bufferCacheHitRatio": "100.00%"
      }
    }
    `
    const { rows, errorsItems, stack } =
      await collectRowsAndErrors(successfulJson)

    assert.equal(rows.length, 7)
    assert.deepEqual(rows[0], 'true')
    assert.deepEqual(rows[1], 'false')
    assert.deepEqual(rows[2], '{"id":1}')
    assert.deepEqual(
      rows[3],
      '{"someCollection":{"id":"6d68232d-c3d1-4d8e-a588-0df9cb03e36e","content":"initial","created_at":"2025-06-03T11:32:56.701Z"}}'
    )
    assert.deepEqual(rows[4], '{}')
    assert.deepEqual(rows[5], 'null')
    assert.deepEqual(rows[6], '"string"')

    assert.isNull(errorsItems)

    const metadata = QueryMetadata.parse(stack.value)
    assert.deepEqual(metadata.requestId, '94c7f89f-92b6-4aba-a90d-be715ca47309')
    assert.isEmpty(metadata.warnings)
    assert.isNotNull(metadata.metrics)
    assert.equal(metadata.metrics.elapsedTime, 14.927542)
    assert.equal(metadata.metrics.executionTime, 12.875792)
    assert.equal(metadata.metrics.resultCount, 2)
    assert.equal(metadata.metrics.resultSize, 2)
    assert.equal(metadata.metrics.processedObjects, 2)
  })

  it('should successfully parse an errored response', async function () {
    const erroredJson = `
    {
      "requestID": "94c7f89f-92b6-4aba-a90d-be715ca47309",
      "signature": { "*": "*" },
      "errors": [
        { "code": 232, "message": "error1" },
        { "code": 233, "message": "error2" }
      ],
      "plans": {},
      "status": "fatal",
      "metrics": {
        "elapsedTime": "14.927542ms",
        "executionTime": "12.875792ms",
        "compileTime": "4.178042ms",
        "queueWaitTime": "0ns",
        "resultCount": 2,
        "resultSize": 2,
        "processedObjects": 2,
        "bufferCacheHitRatio": "100.00%"
      }
    }
    `
    const { rows, errorsItems, stack } = await collectRowsAndErrors(erroredJson)

    assert.equal(rows.length, 0)
    assert.equal(errorsItems!.length, 2)
    assert.equal(errorsItems![0], '{"code":232,"message":"error1"}')
    assert.equal(errorsItems![1], '{"code":233,"message":"error2"}')

    const metadata = QueryMetadata.parse(stack.value)
    assert.deepEqual(metadata.requestId, '94c7f89f-92b6-4aba-a90d-be715ca47309')
    assert.isEmpty(metadata.warnings)
    assert.isNotNull(metadata.metrics)
    assert.equal(metadata.metrics.elapsedTime, 14.927542)
    assert.equal(metadata.metrics.executionTime, 12.875792)
    assert.equal(metadata.metrics.resultCount, 2)
    assert.equal(metadata.metrics.resultSize, 2)
    assert.equal(metadata.metrics.processedObjects, 2)
  })

  it('should successfully parse a mid-stream error response', async function () {
    const midStreamErrorJson = `
    {
      "requestID": "94c7f89f-92b6-4aba-a90d-be715ca47309",
      "signature": { "*": "*" },
      "results": [
        { "id": 1 },
        { "id": 2 }
      ],
      "errors": [
        { "code": 232, "message": "error1" }
      ],
      "plans": {},
      "status": "fatal",
      "metrics": {
        "elapsedTime": "14.927542ms",
        "executionTime": "12.875792ms",
        "compileTime": "4.178042ms",
        "queueWaitTime": "0ns",
        "resultCount": 2,
        "resultSize": 2,
        "processedObjects": 2,
        "bufferCacheHitRatio": "100.00%"
      }
    }
    `
    const { rows, errorsItems, stack } =
      await collectRowsAndErrors(midStreamErrorJson)
    assert.equal(rows.length, 2)
    assert.deepEqual(rows[0], '{"id":1}')
    assert.deepEqual(rows[1], '{"id":2}')
    assert.equal(errorsItems!.length, 1)
    assert.equal(errorsItems![0], '{"code":232,"message":"error1"}')

    const metadata = QueryMetadata.parse(stack.value)
    assert.deepEqual(metadata.requestId, '94c7f89f-92b6-4aba-a90d-be715ca47309')
    assert.isEmpty(metadata.warnings)
    assert.isNotNull(metadata.metrics)
    assert.equal(metadata.metrics.elapsedTime, 14.927542)
    assert.equal(metadata.metrics.executionTime, 12.875792)
    assert.equal(metadata.metrics.resultCount, 2)
    assert.equal(metadata.metrics.resultSize, 2)
    assert.equal(metadata.metrics.processedObjects, 2)
  })

  it('should successfully parse a warning response', async function () {
    const warningResponse = `
    {
      "requestID": "94c7f89f-92b6-4aba-a90d-be715ca47309",
      "signature": { "*": "*" },
      "results": [
        { "id": 1 },
        { "id": 2 }
      ],
      "plans": {},
      "warnings": [
        { "code": 100, "message": "warning1" },
        { "code": 101, "message": "warning2" }
      ],
      "status": "fatal",
      "metrics": {
        "elapsedTime": "14.927542ms",
        "executionTime": "12.875792ms",
        "compileTime": "4.178042ms",
        "queueWaitTime": "0ns",
        "resultCount": 2,
        "resultSize": 2,
        "processedObjects": 2,
        "bufferCacheHitRatio": "100.00%"
      }
    }
    `
    const { rows, errorsItems, stack } =
      await collectRowsAndErrors(warningResponse)
    assert.equal(rows.length, 2)
    assert.deepEqual(rows[0], '{"id":1}')
    assert.deepEqual(rows[1], '{"id":2}')

    const metadata = QueryMetadata.parse(stack.value)
    assert.deepEqual(metadata.requestId, '94c7f89f-92b6-4aba-a90d-be715ca47309')
    assert.equal(metadata.warnings.length, 2)
    assert.equal(metadata.warnings[0].code, 100)
    assert.equal(metadata.warnings[0].message, 'warning1')
    assert.equal(metadata.warnings[1].code, 101)
    assert.equal(metadata.warnings[1].message, 'warning2')
    assert.equal(metadata.metrics.elapsedTime, 14.927542)
    assert.equal(metadata.metrics.executionTime, 12.875792)
    assert.equal(metadata.metrics.resultCount, 2)
    assert.equal(metadata.metrics.resultSize, 2)
    assert.equal(metadata.metrics.processedObjects, 2)
  })

  // These parse simple JSON values, which should essentially just acts as a pass-through onto the stack
  it('should successfully parse a boolean', async function () {
    const data = 'true'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.isTrue(res)
  })

  it('should successfully parse null', async function () {
    const data = 'null'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.isNull(res)
  })

  it('should successfully parse a simple object', async function () {
    const data = `{"a": 1, "b": 2}`
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, { a: 1, b: 2 })
  })

  it('should successfully parse an empty array', async function () {
    const data = '[]'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, [])
  })

  it('should successfully parse a simple array', async function () {
    const data = '[1,2,"three"]'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, [1, 2, 'three'])
  })

  it('should successfully parse an array of objects', async function () {
    const data = '[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ])
  })

  it('should successfully parse an array of mixed types', async function () {
    const data = '[123,"text",true,null,{"key":"value"}]'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, [123, 'text', true, null, { key: 'value' }])
  })

  it('should successfully parse an empty object', async function () {
    const data = '{}'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, {})
  })

  it('should successfully parse an object', async function () {
    const data = '{"name":"John","age":30,"city":"New York"}'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, { name: 'John', age: 30, city: 'New York' })
  })

  it('should successfully parse a nested object', async function () {
    const data = '{"outer":{"inner":{"key":"value"}}}'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, { outer: { inner: { key: 'value' } } })
  })

  it('should successfully parse an object with unicode', async function () {
    const data = '{"name":"你好","city":"Denver"}'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, { name: '你好', city: 'Denver' })
  })

  it('should successfully parse an object with empty key and value', async function () {
    const data = '{"":""}'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, { '': '' })
  })

  it('should successfully parse a complex nested structure', async function () {
    const data =
      '{"users":[{"id":1,"name":"Alice","roles":["admin","editor"]},' +
      '{"id":2,"name":"Bob","roles":["viewer"]}],"meta":{"count":2,"status":"success"}}'
    const { stack } = await collectRowsAndErrors(data)
    let res = JSON.parse(stack.value)
    assert.deepEqual(res, {
      users: [
        { id: 1, name: 'Alice', roles: ['admin', 'editor'] },
        { id: 2, name: 'Bob', roles: ['viewer'] },
      ],
      meta: { count: 2, status: 'success' },
    })
  })

  it('should fail to parse an empty string', async function () {
    const data = ''

    await harness.throwsHelper(async () => {
      await collectRowsAndErrors(data)
    }, Error)
  })

  it('should fail to parse only whitespace', async function () {
    const data = '   \n\t  '

    await harness.throwsHelper(async () => {
      await collectRowsAndErrors(data)
    }, Error)
  })

  it('should fail to parse leading garbage', async function () {
    const data = 'garbage{"key":"value"}'

    await harness.throwsHelper(async () => {
      await collectRowsAndErrors(data)
    }, Error)
  })

  it('should fail to parse trailing garbage', async function () {
    const data = '{"key":"value"}garbage'

    await harness.throwsHelper(async () => {
      await collectRowsAndErrors(data)
    }, Error)
  })

  it('should fail to parse garbage between objects', async function () {
    const data = '[{"id":1,"name":"Alice"},garbage,{"id":2,"name":"Bob"}]'

    await harness.throwsHelper(async () => {
      await collectRowsAndErrors(data)
    }, Error)
  })
})
