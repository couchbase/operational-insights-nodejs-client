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

import { PromiseHelper } from './utilities.js'
import { TimeoutError } from './errors.js'
import { RequestContext } from './requestcontext.js'

/**
 * Represents the behaviour of a request during retries.
 *
 * @internal
 */
export class RequestBehaviour {
  public readonly error: Error
  public readonly retry: boolean
  private constructor(error: Error, retry: boolean) {
    this.retry = retry
    this.error = error
  }

  /**
   * Creates a new RequestBehaviour instance indicating that the request should be retried.
   *
   * @param err The error the caused the retry.
   */
  static retry(err: Error): RequestBehaviour {
    return new RequestBehaviour(err, true)
  }

  /**
   * Creates a new RequestBehaviour instance indicating that the request has failed and should not be retried.
   *
   * @param err The error that caused the failure.
   */
  static fail(err: Error): RequestBehaviour {
    return new RequestBehaviour(err, false)
  }

  /**
   * Indicates whether the request should be retried.
   */
  shouldRetry(): boolean {
    return this.retry
  }

  /**
   * Returns the error that caused the request to fail.
   */
  getError(): Error {
    return this.error
  }
}

/**
 *
 * Helper function that runs a function with retry logic based on the provided evaluation function.
 *
 * @param fn The function to execute, which returns a Promise. It receives a
 *   signal that is aborted when the attempt times out, and must stop its
 *   request when that happens.
 * @param evaluate A function that evaluates the error and returns a RequestBehaviour indicating whether to retry or fail.
 * @param deadline The deadline timestamp by which the operation must complete.
 * @param requestContext The context for the request, used for error messages.
 */
export async function runWithRetry<T>(
  fn: (attemptSignal: AbortSignal) => Promise<T>,
  evaluate: (errs: any) => RequestBehaviour,
  deadline: number,
  requestContext: RequestContext
): Promise<T> {
  let lastErr: Error | null = null

  for (
    let retryIdx = 0;
    retryIdx <= requestContext.maxRetryAttempts;
    retryIdx++
  ) {
    const remainingTime = deadline - Date.now()
    if (remainingTime <= 0) {
      requestContext.addPreviousAttemptErrorToContext(lastErr)
      throw new TimeoutError(
        requestContext.attachErrorContext('Query timeout exceeded')
      )
    }

    const attemptController = new AbortController()
    try {
      requestContext.incrementAttempt()
      return await PromiseHelper.promiseWithTimeout(
        fn(attemptController.signal),
        remainingTime,
        (err) => attemptController.abort(err)
      )
    } catch (err) {
      // TimeoutError from promiseWithTimeout is handled separately
      if (err instanceof TimeoutError) {
        requestContext.addPreviousAttemptErrorToContext(lastErr)
        throw new TimeoutError(requestContext.attachErrorContext(err.message))
      }

      const behaviour = evaluate(err)
      if (!behaviour.shouldRetry()) {
        throw behaviour.getError()
      }

      lastErr = behaviour.getError()
    }

    const delay = calculateBackoff(requestContext.numAttempts)
    if (Date.now() + delay > deadline) {
      requestContext.addPreviousAttemptErrorToContext(lastErr)
      throw new TimeoutError(
        requestContext.attachErrorContext(
          'Query timeout will exceed during retry backoff'
        )
      )
    }
    await sleep(delay)
  }

  throw lastErr
}

/**
 * Calculates an exponential backoff delay based on the retry count.
 *
 * @param retryCount The number of retries that have been attempted.
 */
function calculateBackoff(retryCount: number) {
  const baseDelay = 100
  const maxDelay = 60_000

  const exponentialDelay = baseDelay * Math.pow(2, retryCount)
  const cappedDelay = Math.min(exponentialDelay, maxDelay)

  return Math.random() * cappedDelay
}

/**
 * @internal
 */
function sleep(delay: number) {
  return new Promise((resolve) => setTimeout(resolve, delay))
}
