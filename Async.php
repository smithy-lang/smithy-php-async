<?php

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

declare(strict_types=1);

namespace Smithy\Async;

/**
 * Concurrency primitives for Smithy clients.
 *
 * This stateless utility class provides entry points for explicit concurrent
 * work. Each operation runs in its own fiber and cooperates when it reaches
 * asynchronous I/O or calls {@see self::delay()}.
 */
final class Async
{
    private static ?EventLoopInterface $eventLoop = null;

    private function __construct() {}

    /**
     * Run callables concurrently in separate fibers.
     *
     * Block until every callable completes and return results in input order.
     * Work cooperates when a callable invokes {@see self::delay()} or another
     * component suspends through the configured event loop.
     *
     * @param callable(): mixed ...$operations
     *
     * @return list<mixed> Results in input order.
     * @throws \Throwable When an operation fails.
     */
    public static function all(callable ...$operations): array
    {
        return self::resolveEventLoop()->run($operations);
    }

    /**
     * Apply a callable to iterable items with bounded concurrency.
     *
     * @template TKey
     * @template TValue
     * @template TResult
     *
     * @param iterable<TKey, TValue> $items Items to process.
     * @param callable(TValue, TKey): TResult $fn Callable applied to each item.
     * @param int $concurrency Maximum concurrent operations. Defaults to 25.
     *
     * @return list<TResult> Results in input order.
     * @throws \InvalidArgumentException When concurrency is less than one.
     * @throws \Throwable When item processing fails.
     */
    public static function map(
        iterable $items,
        callable $fn,
        int $concurrency = 25
    ): array
    {
        if ($concurrency < 1) {
            throw new \InvalidArgumentException(
                'Concurrency must be at least 1.'
            );
        }

        if ($items === []) {
            return [];
        }

        $loop = self::resolveEventLoop();
        $itemReader = self::createItemReader($loop, self::iterate($items));
        $results = [];
        $workers = [];

        for ($i = 0; $i < $concurrency; $i++) {
            $workers[] = static function () use (
                $fn,
                $itemReader,
                &$results
            ): void {
                while (($item = $itemReader()) !== null) {
                    [$index, $key, $value] = $item;
                    $results[$index] = $fn($value, $key);
                }
            };
        }

        $loop->run($workers);
        \ksort($results);

        return \array_values($results);
    }

    /**
     * Configure the global event loop.
     *
     * Use {@see RevoltEventLoop} when no custom loop is configured.
     *
     * @return void
     */
    public static function configure(EventLoopInterface $loop): void
    {
        self::$eventLoop = $loop;
    }

    /**
     * Delay the current flow using the configured event loop.
     *
     * A zero-or-negative delay is a no-op.
     *
     * @return void
     */
    public static function delay(float $seconds): void
    {
        if ($seconds <= 0.0) {
            return;
        }

        self::resolveEventLoop()->delay($seconds);
    }

    private static function resolveEventLoop(): EventLoopInterface
    {
        return self::$eventLoop ??= new RevoltEventLoop();
    }

    /**
     * Create a reader that serializes access to a shared iterator.
     *
     * @template TKey
     * @template TValue
     *
     * @param \Iterator<TKey, TValue> $iterator
     *
     * @return \Closure(): (array{0: int, 1: TKey, 2: TValue}|null)
     */
    private static function createItemReader(
        EventLoopInterface $loop,
        \Iterator $iterator
    ): \Closure
    {
        $nextResultIndex = 0;
        $started = false;
        $done = false;
        $locked = false;
        $waiters = [];

        return static function () use (
            $loop,
            $iterator,
            &$nextResultIndex,
            &$started,
            &$done,
            &$locked,
            &$waiters
        ): ?array {
            self::acquireIteratorLock($loop, $locked, $waiters);

            try {
                if ($done) {
                    return null;
                }

                if ($started) {
                    $iterator->next();
                } else {
                    $started = true;
                }

                if (!$iterator->valid()) {
                    $done = true;

                    return null;
                }

                return [
                    $nextResultIndex++,
                    $iterator->key(),
                    $iterator->current()
                ];
            } finally {
                self::releaseIteratorLock($loop, $locked, $waiters);
            }
        };
    }

    /**
     * Acquire exclusive iterator access.
     *
     * @param list<callable(): void> $waiters
     *
     * @return void
     */
    private static function acquireIteratorLock(
        EventLoopInterface $loop,
        bool &$locked,
        array &$waiters
    ): void
    {
        if (!$locked) {
            $locked = true;

            return;
        }

        $loop->suspend(
            static function (callable $resume) use (&$waiters): void {
                $waiters[] = $resume;
            }
        );
    }

    /**
     * Release exclusive iterator access.
     *
     * @param list<callable(): void> $waiters
     *
     * @return void
     */
    private static function releaseIteratorLock(
        EventLoopInterface $loop,
        bool &$locked,
        array &$waiters
    ): void
    {
        if ($waiters === []) {
            $locked = false;

            return;
        }

        $resume = \array_shift($waiters);
        $loop->defer(
            static function () use ($resume): void {
                $resume();
            }
        );
    }

    /**
     * Lazily iterate over any iterable input.
     *
     * @template TKey
     * @template TValue
     *
     * @param iterable<TKey, TValue> $items
     *
     * @return \Generator<TKey, TValue>
     */
    private static function iterate(iterable $items): \Generator
    {
        foreach ($items as $key => $value) {
            yield $key => $value;
        }
    }
}
