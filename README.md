# Smithy PHP Async

Fiber-based concurrency helpers and an event-loop contract for Smithy PHP
clients. Requires PHP 8.2 or later and `revolt/event-loop`.

## Installation

```shell
composer require smithy/async
```

## Concurrent Operations

`Async::all()` runs each callable in its own fiber and returns results in input
order.

```php
use Smithy\Async\Async;

[$account, $orders] = Async::all(
    fn() => $client->getAccount(new GetAccountInput()),
    fn() => $client->listOrders(new ListOrdersInput())
);
```

`Async::map()` applies bounded concurrency to an iterable. Its default
concurrency is 25.

```php
$results = Async::map(
    $recordIds,
    fn(string $id) => $client->getRecord(
        new GetRecordInput(id: $id)
    ),
    concurrency: 10
);
```

Callbacks make concurrent progress when they suspend at an asynchronous I/O
operation or call `Async::delay()`.

## Event Loops

`RevoltEventLoop` supplies the default implementation. Call
`Async::configure()` to install another `EventLoopInterface` implementation
before starting concurrent work.

```php
use Smithy\Async\Async;
use Smithy\Async\EventLoopInterface;

Async::configure($eventLoop);
```

`EventLoopInterface` covers fiber suspension, timers, deferred callbacks, and
running callback groups. `Async::delay()` uses the configured loop and returns
immediately for zero or negative delays.

## Package Boundary

This package contains scheduling and concurrency primitives. HTTP transfer
cancellation belongs to `smithy/http`.

## Design

See [Async Runtime and HTTP Concurrency](../../docs/design/async/async-runtime-and-http-concurrency.md).

## Contributing

Run the component tests from the monorepo root:

```shell
vendor/bin/phpunit tests/Async
```
