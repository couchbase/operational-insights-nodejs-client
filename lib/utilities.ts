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

import { TimeoutError } from './errors.js'

/**
 * Reprents a node-style callback which receives an optional error or result.
 *
 * @category Utilities
 */
export interface NodeCallback<T> {
  (err: Error | null, result: T | null): void
}

/**
 * @internal
 */
export class PromiseHelper {
  /**
   * Helper function to run a promise with a timeout.
   *
   * @param promise The promise to run.
   * @param ms The timeout in milliseconds.
   * @param onTimeout Called when the timeout fires, to cancel the work behind
   *   the promise.
   */
  static async promiseWithTimeout<T>(
    promise: Promise<T>,
    ms: number,
    onTimeout?: (err: TimeoutError) => void
  ): Promise<T> {
    let timeoutId
    const timeout = new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => {
        const err = new TimeoutError(`Operation timed out after ${ms}ms`)
        reject(err)
        onTimeout?.(err)
      }, ms)
    })

    try {
      return await Promise.race([promise, timeout])
    } finally {
      clearTimeout(timeoutId)
    }
  }
}

/**
 * @internal
 */
export class ParsingUtilities {
  /**
   * @internal
   */
  static parseGolangSyntaxDuration(value: string): number {
    const unitToNanos: Record<string, number> = {
      ns: 1,
      us: 1e3,
      µs: 1e3,
      μs: 1e3,
      ms: 1e6,
      s: 1e9,
      m: 60e9,
      h: 3600e9,
    }

    const negative = value.startsWith('-')
    const abs = negative || value.startsWith('+') ? value.slice(1) : value

    if (negative) {
      throw new Error(`Negative durations are not supported: "${value}"`)
    }

    if (abs.length === 0) {
      throw new Error(`Invalid duration: "${value}"`)
    }

    if (abs === '0') return 0

    const componentPattern =
      /((?:\d*\.\d+)|(?:\d+\.?\d*))(ns|us|μs|µs|ms|s|m|h)/g
    let totalNanos = 0
    let match: RegExpExecArray | null

    let parsed = 0
    while ((match = componentPattern.exec(abs)) !== null) {
      const [, numStr, unit] = match
      const multiplier = unitToNanos[unit]
      if (!multiplier) {
        throw new Error(`Unknown unit "${unit}" in duration: "${value}"`)
      }

      const num = parseFloat(numStr)
      if (isNaN(num)) {
        throw new Error(`Invalid number "${numStr}" in duration: "${value}"`)
      }

      totalNanos += num * multiplier
      parsed += match[0].length
    }

    if (parsed !== abs.length) {
      throw new Error(`Invalid duration: "${value}"`)
    }

    const millis = totalNanos / 1e6
    if (!Number.isFinite(millis)) {
      throw new Error(`Duration "${value}" is too large.`)
    }

    return millis
  }
}
