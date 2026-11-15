<?php

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

declare(strict_types=1);

namespace Smithy\Async;

/**
 * Abstraction over an event loop implementation.
 *
 * Implementations provide fiber suspension, timers, deferred callbacks, and
 * callback group execution for async-capable transports.
 */
interface EventLoopInterface
{
    /**
     * Run callbacks concurrently in separate fibers.
     *
     * Block until every callback completes.
     *
     * @param array<array-key, callable(): mixed> $callbacks
     *
     * @return list<mixed> Results in input order.
     * @throws \Throwable When a callback fails.
     */
    public function run(array $callbacks): array;

    /**
     * Suspend the current fiber until resumed.
     *
     * @param \Closure(\Closure(mixed=): void, \Closure(\Throwable): void): void $onSuspend
     *
     * @return mixed Value passed to the resume callback.
     */
    public function suspend(\Closure $onSuspend): mixed;

    /**
     * Delay the current flow.
     *
     * Suspend through the event loop rather than blocking the process.
     *
     * @return void
     */
    public function delay(float $seconds): void;

    /**
     * Schedule a callback after the given number of seconds.
     *
     * @param callable(): void $callback
     *
     * @return string Identifier that can cancel the callback.
     */
    public function schedule(float $seconds, callable $callback): string;

    /**
     * Cancel a scheduled callback.
     *
     * @return void
     */
    public function cancel(string $callbackId): void;

    /**
     * Schedule a callback to run on the next tick of the event loop.
     *
     * @param callable(): void $callback
     *
     * @return void
     */
    public function defer(callable $callback): void;
}
