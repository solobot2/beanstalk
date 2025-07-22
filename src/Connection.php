<?php

namespace Amp\Beanstalk;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Socket\ConnectContext;
use Amp\Socket\ConnectException;
use Amp\Socket\Socket;
use Amp\Uri\Uri;
use Revolt\EventLoop;

use function Amp\Socket\socketConnector;

class Connection
{
    /** @var DeferredFuture|null */
    private ?DeferredFuture $connectPromisor = null;

    /** @var Parser */
    private Parser $parser;

    /** @var int */
    private int $timeout = 5000;

    /** @var Socket|null */
    private ?Socket $socket = null;

    /** @var string */
    private string $uri;

    /** @var callable[][] */
    private array $handlers;

    public function __construct(string $uri)
    {
        $this->applyUri($uri);
        $this->handlers = [
            "connect"  => [],
            "response" => [],
            "error"    => [],
            "close"    => [],
        ];

        $this->parser = new Parser(function ($response) {
            foreach ($this->handlers["response"] as $handler) {
                $handler($response);
            }

            if ($response instanceof BadFormatException) {
                $this->onError($response);
            }
        });
    }

    private function applyUri(string $uri): void
    {
        $uri = new Uri($uri);

        $this->timeout = (int)($uri->getQueryParameter("timeout") ?? $this->timeout);
        $this->uri = $uri->getScheme() . "://" . $uri->getHost() . ":" . $uri->getPort();
    }

    public function addEventHandler($events, callable $callback): void
    {
        $events = (array)$events;

        foreach ($events as $event) {
            if (!isset($this->handlers[$event])) {
                throw new \Error("Unknown event: " . $event);
            }

            $this->handlers[$event][] = $callback;
        }
    }

    public function send(string $payload): Future
    {
        return \Amp\async(function () use ($payload) {
            $this->connect()->await();
            $this->socket->write($payload);
        });
    }

    private function connect(): Future
    {
        if ($this->connectPromisor) {
            return $this->connectPromisor->getFuture();
        }

        if ($this->socket) {
            return Future::complete(null);
        }

        $this->connectPromisor = new DeferredFuture();
        $connector = socketConnector();

        EventLoop::queue(function () use ($connector) {
            try {
                $this->socket = $connector->connect($this->uri, (new ConnectContext())->withConnectTimeout($this->timeout));
                foreach ($this->handlers["connect"] as $handler) {
                    $pipelinedCommand = $handler();

                    if (!empty($pipelinedCommand)) {
                        $this->socket->write($pipelinedCommand);
                    }
                }

                EventLoop::queue(function () {
                    while (null !== $chunk = $this->socket->read()) {
                        $this->parser->send($chunk);
                    }
                    $this->close();
                });

                $this->connectPromisor->complete();
            } catch (\Throwable $e) {
                $this->connectPromisor->error(
                    new ConnectException(
                        "Connection attempt failed",
                        0,
                        $e
                    )
                );
                $this->connectPromisor = null;
            }
        });

        return $this->connectPromisor->getFuture();
    }

    private function onError(\Throwable $exception): void
    {
        foreach ($this->handlers["error"] as $handler) {
            $handler($exception);
        }

        $this->close();
    }

    public function close(): void
    {
        $this->parser->reset();

        if ($this->socket) {
            $this->socket->close();
            $this->socket = null;
        }

        foreach ($this->handlers["close"] as $handler) {
            $handler();
        }
    }

    public function __destruct()
    {
        $this->close();
    }
}
