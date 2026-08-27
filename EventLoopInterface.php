<?php

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
     * Run multiple callables concurrently, each in its own fiber.
     * Blocks the caller until all callables have completed.
     *
     * @param array<callable(): mixed> $callbacks
     * @return array<mixed> Results in the same order as the input callables.
     *
     * @throws \Throwable If any callable throws, its exception propagates.
     */
    public function run(array $callbacks): array;

    /**
     * Suspend the current fiber until the event loop resumes it.
     * This is called internally by async-capable HTTP handlers
     * when waiting on I/O.
     *
     * @param \Closure $onSuspend Receives two callbacks:
     *                 - resume(mixed $value): Resume the fiber with a value.
     *                 - throw(\Throwable $e): Resume the fiber by throwing.
     * @return mixed The value passed to resume().
     */
    public function suspend(\Closure $onSuspend): mixed;

    /**
     * Delay the current flow for the given number of seconds.
     *
     * Implementations should suspend using the underlying event loop
     * rather than busy-waiting or sleeping the process.
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
