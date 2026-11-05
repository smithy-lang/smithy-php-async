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
        $states = [];
        $pending = \count($callbacks);

        if ($pending === 0) {
            return [];
        }

        $suspension = Revolt::getSuspension();

        foreach ($callbacks as $index => $callback) {
            $state = new \stdClass();
            $state->result = null;
            $state->exception = null;
            // Keep the fiber on state for future cancellation or introspection.
            $state->fiber = null;
            $states[$index] = $state;
            $fiber = new \Fiber(function () use (
                $callback,
                $state,
                &$pending,
                $suspension
            ): void {
                try {
                    $result = $callback();
                    $state->result = $result;
                } catch (\Throwable $e) {
                    $state->exception = $e;
                } finally {
                    $state->fiber = null;
                    if (--$pending === 0) {
                        $suspension->resume();
                    }
                }
            });
            $state->fiber = $fiber;
            Revolt::queue(
                static function () use ($fiber): void {
                    $fiber->start();
                }
            );
        }

        $suspension->suspend();

        $results = [];
        $exceptions = [];
        foreach ($states as $index => $state) {
            if ($state->exception !== null) {
                $exceptions[$index] = $state->exception;
            } else {
                $results[$index] = $state->result;
            }
        }

        if ($exceptions !== []) {
            throw \reset($exceptions);
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
        return Revolt::delay(
            \max(0.0, $seconds),
            static function () use ($callback): void {
                $callback();
            }
        );
    }

    /** @inheritDoc */
    public function cancel(string $callbackId): void
    {
        Revolt::cancel($callbackId);
    }

    /** @inheritDoc */
    public function defer(callable $callback): void
    {
        Revolt::defer(static function () use ($callback): void {
            $callback();
        });
    }
}
