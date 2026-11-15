<?php

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

declare(strict_types=1);

namespace Smithy\Async;

use Revolt\EventLoop as Revolt;

/**
 * Adapt event-loop operations to Revolt.
 */
final class RevoltEventLoop implements EventLoopInterface
{
    /** @inheritDoc */
    public function run(array $callbacks): array
    {
        $pending = \count($callbacks);

        if ($pending === 0) {
            return [];
        }

        $suspension = Revolt::getSuspension();
        $results = [];
        $errors = [];

        foreach ($callbacks as $index => $callback) {
            // Seed the slot so results keep input order.
            $results[$index] = null;
            $fiber = new \Fiber(static function () use (
                $callback,
                $index,
                &$results,
                &$errors,
                &$pending,
                $suspension
            ): void {
                try {
                    $results[$index] = $callback();
                } catch (\Throwable $e) {
                    $errors[$index] = $e;
                } finally {
                    if (--$pending === 0) {
                        $suspension->resume();
                    }
                }
            });
            Revolt::queue($fiber->start(...));
        }

        $suspension->suspend();

        if ($errors !== []) {
            // Throw the first failure in input order, not completion order.
            foreach ($callbacks as $index => $_) {
                if (isset($errors[$index])) {
                    throw $errors[$index];
                }
            }
        }

        return \array_values($results);
    }

    /** @inheritDoc */
    public function suspend(\Closure $onSuspend): mixed
    {
        $suspension = Revolt::getSuspension();

        Revolt::queue(
            static function () use ($onSuspend, $suspension): void {
                $settled = false;
                $resume = static function (
                    mixed $value = null
                ) use ($suspension, &$settled): void {
                    if ($settled) {
                        return;
                    }

                    $settled = true;
                    $suspension->resume($value);
                };
                $throw = static function (
                    \Throwable $e
                ) use ($suspension, &$settled): void {
                    if ($settled) {
                        return;
                    }

                    $settled = true;
                    $suspension->throw($e);
                };

                try {
                    $onSuspend($resume, $throw);
                } catch (\Throwable $e) {
                    $throw($e);
                }
            }
        );

        return $suspension->suspend();
    }

    /** @inheritDoc */
    public function delay(float $seconds): void
    {
        if ($seconds <= 0.0) {
            return;
        }

        $suspension = Revolt::getSuspension();
        Revolt::delay($seconds, static function () use ($suspension): void {
            $suspension->resume();
        });
        $suspension->suspend();
    }

    /** @inheritDoc */
    public function schedule(
        float $seconds,
        callable $callback
    ): string
    {
        return Revolt::delay(\max(0.0, $seconds), self::toClosure($callback));
    }

    /** @inheritDoc */
    public function cancel(string $callbackId): void
    {
        Revolt::cancel($callbackId);
    }

    /** @inheritDoc */
    public function defer(callable $callback): void
    {
        Revolt::defer(self::toClosure($callback));
    }

    /**
     * Adapt a callback for Revolt so it runs with no arguments.
     *
     * Revolt passes the callback ID as an argument. The wrapper drops it,
     * so typed, variadic and func_get_args() callbacks and internal
     * functions never see it. One closure costs about the same as the
     * reflection a pass-through check would need.
     */
    private static function toClosure(callable $callback): \Closure
    {
        return static function () use ($callback): void {
            $callback();
        };
    }
}
