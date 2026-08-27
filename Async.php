<?php

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
    /** @var ?EventLoopInterface Globally configured event loop. */
    private static ?EventLoopInterface $eventLoop = null;

    private function __construct() {}

    /**
     * Run multiple callables concurrently, each in its own fiber.
     * Blocks the caller until all callables have completed.
     * Results are returned in the same order as the input.
     *
     * Each callable receives its own fiber. Work cooperates when it calls
     * Async::delay() or a transport/runtime component suspends through the
     * configured event loop.
     *
     * @param callable(): mixed ...$operations
     * @return array<mixed> Results in input order.
     *
     * @throws \RuntimeException If no event loop is available.
     * @throws \Throwable Re-throws the first exception from any operation.
     */
    public static function all(callable ...$operations): array
    {
        return self::resolveEventLoop()->run($operations);
    }

    /**
     * Apply a callable to each item in an iterable with bounded
     * concurrency. At most $concurrency operations run simultaneously.
     *
     * Use this to process a large set of operations without overwhelming
     * connections or hitting API throttling limits.
     *
     * @template TKey
     * @template TValue
     * @template TResult
     *
     * @param iterable<TKey, TValue> $items Items to process.
     * @param callable(TValue, TKey): TResult $fn Callable applied to each item.
     * @param int $concurrency Maximum concurrent operations. Defaults to 25.
     *
     * @return array<TResult> Results in the same order as the input.
     *
     * @throws \RuntimeException If no event loop is available.
     * @throws \Throwable Re-throws the first exception from any operation.
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

        $loop = self::resolveEventLoop();
        $itemReader = self::createItemReader($loop, self::iterate($items));
        $workers = [];

        for ($i = 0; $i < $concurrency; $i++) {
            $workers[] = static function () use (
                $fn,
                $itemReader
            ): array {
                $workerResults = [];

                while (($item = $itemReader()) !== null) {
                    [$index, $key, $value] = $item;
                    $result = $fn($value, $key);
                    $workerResults[] = [$index, $result];
                }

                return $workerResults;
            };
        }

        $workerResults = $loop->run($workers);
        $results = [];
        foreach ($workerResults as $workerResult) {
            foreach ($workerResult as [$index, $result]) {
                $results[$index] = $result;
            }
        }

        ksort($results);

        return array_values($results);
    }

    /**
     * Configure the global event loop.
     *
     * Customers who want to use a custom event loop implementation
     * call this before making any async SDK calls.
     *
     * If never called, Async auto-discovers Revolt.
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

    /**
     * Resolve the event loop.
     *
     * Returns the configured loop, or creates a RevoltEventLoop
     * on first call.
     */
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
