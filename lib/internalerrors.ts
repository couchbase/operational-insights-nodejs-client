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

/**
 * Indicates that we failed to connect to a node within the timeout period, but still want to retry if possible.
 *
 * @internal
 */
export class InternalConnectionTimeout extends Error {
  constructor() {
    super('Timed out waiting to connect to node')
  }
}

/**
 * Internal wrapper to indicate that the server returned an errored status code.
 *
 * @internal
 */
export class HttpStatusError extends Error {
  private _statusCode: number

  constructor(statusCode: number) {
    super('HttpStatusCode error: ' + statusCode)
    this.name = this.constructor.name
    this._statusCode = statusCode
  }

  /**
   * @internal
   */
  get statusCode(): number {
    return this._statusCode
  }
}

/**
 * Internal wrapper which represents an error that was emitted either by the HTTP request or response
 *
 * @internal
 */
export class ConnectionError extends Error {
  private request: boolean
  cause: Error

  constructor(err: Error, request: boolean) {
    super(`ConnectionError: ${err.message}`)
    this.name = this.constructor.name
    this.cause = err
    this.request = request
  }

  /**
   * @internal
   */
  get isRequestError(): boolean {
    return this.request
  }
}

/**
 * @internal
 */
export class ErrorContext {
  lastDispatchedTo: string | undefined
  lastDispatchedFrom: string | undefined
  path: string | undefined
  method: string | undefined
  statusCode: number | undefined
  statement: string | undefined
  previousAttemptErrors: any
  otherServerErrors: any[] = []
  numAttempts: number = 0

  /**
   * @internal
   */
  toString(): string {
    const parts: string[] = []
    if (this.lastDispatchedTo)
      parts.push(`lastDispatchedTo=${this.lastDispatchedTo}`)
    if (this.lastDispatchedFrom)
      parts.push(`lastDispatchedFrom=${this.lastDispatchedFrom}`)
    if (this.method) parts.push(`method=${this.method}`)
    if (this.path) parts.push(`path=${this.path}`)
    if (this.statusCode) parts.push(`statusCode=${this.statusCode}`)
    if (this.statement) parts.push(`statement=${this.statement}`)
    if (this.previousAttemptErrors)
      parts.push(`previousAttemptErrors=${this.previousAttemptErrors}`)
    if (this.numAttempts) parts.push(`numAttempts=${this.numAttempts}`)
    if (this.otherServerErrors.length > 0)
      parts.push(`otherServerErrors=${JSON.stringify(this.otherServerErrors)}`)
    return `ErrorContext: ${parts.join(', ')}`
  }
}
