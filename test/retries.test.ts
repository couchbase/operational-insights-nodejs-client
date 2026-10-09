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

import { assert } from 'chai'
import * as net from 'node:net'
import { runWithRetry, RequestBehaviour } from '../lib/retries.js'
import { harness } from './harness.js'
import { TimeoutError } from '../lib/errors.js'
import { RequestContext } from '../lib/requestcontext.js'
import { Credential, createInstance } from '../lib/operationalinsights.js'

describe('#Retries', function () {
  it('should retry on retriable errors and succeed eventually', async function () {
    let callCount = 0
    const failAttempts = 2

    const fn = async (): Promise<string> => {
      callCount++
      if (callCount <= failAttempts) {
        throw new Error('Temporary failure')
      }
      return 'success'
    }

    const evaluate = (err: Error) => {
      if (err.message === 'Temporary failure') {
        return RequestBehaviour.retry(err)
      }
      return RequestBehaviour.fail(err)
    }

    const result = await runWithRetry(
      fn,
      evaluate,
      Date.now() + 50000,
      new RequestContext(7)
    )
    assert.equal(result, 'success')
    assert.equal(callCount, failAttempts + 1)
  })

  it('should fail if deadline is exceeded', async function () {
    const fn = async (): Promise<never> => {
      throw new Error('Temporary failure')
    }

    const evaluate = (err: any) => RequestBehaviour.retry(err)

    await harness.throwsHelper(async () => {
      await runWithRetry(fn, evaluate, Date.now() + 500, new RequestContext(7))
    }, TimeoutError)
  })

  it('should fail immediately on fatal error', async function () {
    let callCount = 0

    const fn = async (): Promise<never> => {
      callCount++
      throw new Error('Fatal')
    }
    const evaluate = (err: Error) => RequestBehaviour.fail(err)

    await harness.throwsHelper(async () => {
      await runWithRetry(fn, evaluate, Date.now() + 500, new RequestContext(7))
    }, Error)

    assert.equal(callCount, 1)
  })

  it('should fail with the final error if retries are exceeded', async function () {
    this.timeout(5000)

    let callCount = 0
    const context = new RequestContext(3)

    const fn = async (): Promise<never> => {
      callCount++
      throw new Error('Temporary failure')
    }

    const evaluate = (errs: any) => {
      return RequestBehaviour.retry(errs)
    }

    try {
      await runWithRetry(fn, evaluate, Date.now() + 5000, context)
      assert(false)
    } catch (e) {
      assert.instanceOf(e, Error)
      assert.include(e.message, 'Temporary failure')
      assert.equal(callCount, 4)
    }
  })

  it('should abort the attempt and add request context when an attempt times out', async function () {
    let attemptSignal: AbortSignal | undefined

    const fn = (signal: AbortSignal): Promise<never> => {
      attemptSignal = signal
      return new Promise<never>(() => undefined)
    }
    const evaluate = (err: any) => RequestBehaviour.retry(err)
    const context = new RequestContext(3)
    context.setGenericRequestContextFields(
      'SELECT 1',
      '/api/v1/request',
      'POST'
    )

    try {
      await runWithRetry(fn, evaluate, Date.now() + 200, context)
      assert.fail('expected a TimeoutError')
    } catch (e) {
      assert.instanceOf(e, TimeoutError)
      assert.include(e.message, 'statement=SELECT 1')
      assert.include(e.message, 'numAttempts=1')
    }
    assert.isTrue(attemptSignal?.aborted)
    assert.instanceOf(attemptSignal?.reason, TimeoutError)
  })
})

describe('#Attempt timeouts', function () {
  // A server that accepts connections and never replies, so every attempt
  // runs until its timeout.
  let server: net.Server
  let sockets: net.Socket[]
  let port: number

  beforeEach(async function () {
    sockets = []
    server = net.createServer((socket) => {
      sockets.push(socket)
      socket.on('error', () => undefined)
      // Read and discard the request, otherwise the socket never sees the
      // client's FIN.
      socket.resume()
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    port = (server.address() as net.AddressInfo).port
  })

  afterEach(async function () {
    sockets.forEach((socket) => socket.destroy())
    await new Promise<void>((resolve) => server.close(() => resolve()))
  })

  async function assertAllClosed(): Promise<void> {
    assert.isNotEmpty(sockets)
    await Promise.all(
      sockets.map(
        (socket) =>
          new Promise<void>((resolve, reject) => {
            if (socket.closed) return resolve()
            const timer = setTimeout(
              () => reject(new Error('connection was left open')),
              1000
            )
            socket.once('close', () => {
              clearTimeout(timer)
              resolve()
            })
          })
      )
    )
  }

  it('closes the connection when executeQuery times out', async function () {
    this.timeout(5000)
    const cluster = createInstance(
      `http://127.0.0.1:${port}`,
      new Credential('u', 'p')
    )
    try {
      await harness.throwsHelper(async () => {
        await cluster.executeQuery('SELECT 1', { timeout: 300 })
      }, TimeoutError)
      await assertAllClosed()
    } finally {
      cluster.close()
    }
  })

  it('closes the connection when startQuery times out', async function () {
    this.timeout(5000)
    const cluster = createInstance(
      `http://127.0.0.1:${port}`,
      new Credential('u', 'p')
    )
    try {
      await harness.throwsHelper(async () => {
        await cluster.startQuery('SELECT 1', { timeout: 300 })
      }, TimeoutError)
      await assertAllClosed()
    } finally {
      cluster.close()
    }
  })

  it('does not send the request when the attempt times out while resolving the host', async function () {
    this.timeout(5000)
    const cluster = createInstance(
      `http://127.0.0.1:${port}`,
      new Credential('u', 'p')
    )
    // Stand in for a slow DNS lookup that outlasts the attempt timeout.
    const httpClient = cluster.httpClient
    const requestOptions = httpClient.requestOptions.bind(httpClient)
    httpClient.requestOptions = async () => {
      await new Promise((resolve) => setTimeout(resolve, 500))
      return requestOptions()
    }
    try {
      await harness.throwsHelper(async () => {
        await cluster.executeQuery('SELECT 1', { timeout: 300 })
      }, TimeoutError)
      await new Promise((resolve) => setTimeout(resolve, 500))
      assert.isEmpty(sockets)
    } finally {
      cluster.close()
    }
  })

  it('closes the connection when a request times out after the query was aborted', async function () {
    this.timeout(5000)
    // Answer startQuery so there is a handle to use; later requests hang.
    const body = JSON.stringify({ requestID: 'r1', handle: '/api/v1/r1' })
    server.once('connection', (socket) => {
      socket.once('data', () => {
        socket.write(
          'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n' +
            `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`
        )
      })
    })
    const cluster = createInstance(
      `http://127.0.0.1:${port}`,
      new Credential('u', 'p')
    )
    const controller = new AbortController()
    try {
      const handle = await cluster.startQuery('SELECT 1', {
        timeout: 300,
        abortSignal: controller.signal,
      })
      // An aborted executor still sends follow-up requests, the same as a
      // QueryResultHandle reused after QueryResult.cancel().
      controller.abort()
      await harness.throwsHelper(async () => {
        await handle.cancel()
      }, TimeoutError)
      await assertAllClosed()
    } finally {
      cluster.close()
    }
  })
})
