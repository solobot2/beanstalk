<?php

namespace Amp\Beanstalk;

use Amp\Beanstalk\Stats\Job;
use Amp\Beanstalk\Stats\System;
use Amp\Beanstalk\Stats\Tube;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Uri\Uri;
use Symfony\Component\Yaml\Yaml;
use Throwable;

use function Amp\async;

class BeanstalkClient
{
    /** @var DeferredFuture[] */
    private array $deferreds = [];

    /** @var Connection */
    private Connection $connection;

    /** @var string|null */
    private ?string $tube = null;

    public function __construct(string $uri)
    {
        $this->applyUri($uri);

        $this->connection = new Connection($uri);
        $this->connection->addEventHandler("response", function ($response) {
            /** @var DeferredFuture $deferred */
            $deferred = array_shift($this->deferreds);

            if ($response instanceof Throwable) {
                $deferred->error($response);
            } else {
                $deferred->complete($response);
            }
        });

        $this->connection->addEventHandler("error", function (?Throwable $error = null) {
            if ($error) {
                $this->failAllDeferreds($error);
            }
        });
        $this->connection->addEventHandler("close", function () {
            $this->failAllDeferreds(new ConnectionClosedException("Connection closed"));
        });

        if ($this->tube) {
            $this->connection->addEventHandler("connect", function () {
                array_unshift($this->deferreds, new DeferredFuture());

                return "use $this->tube\r\n";
            });
        }
    }

    private function applyUri(string $uri): void
    {
        $this->tube = (new Uri($uri))->getQueryParameter("tube");
    }

    private function send(string $message, ?callable $transform = null): Future
    {
        return async(function () use ($message, $transform) {
            $this->deferreds[] = $deferred = new DeferredFuture();
            $promise = $deferred->getFuture();

            $this->connection->send($message)->await();
            $response = $promise->await();

            return $transform ? $transform($response) : $response;
        });
    }

    public function use(string $tube): Future
    {
        return $this->send("use " . $tube . "\r\n", function () use ($tube) {
            $this->tube = $tube;
            return null;
        });
    }

    public function pause(string $tube, int $delay): Future
    {
        $payload = "pause-tube $tube $delay\r\n";

        return $this->send($payload, function (array $response) use ($tube) {
            [$type] = $response;

            switch ($type) {
                case "PAUSED":
                    return null;

                case "NOT_FOUND":
                    throw new NotFoundException("Tube with name $tube is not found");

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function put(string $payload, int $timeout = 60, int $delay = 0, $priority = 0): Future
    {
        $payload = "put $priority $delay $timeout " . strlen($payload) . "\r\n$payload\r\n";

        return $this->send($payload, function (array $response): int {
            [$type] = $response;

            switch ($type) {
                case "INSERTED":
                case "BURIED":
                    return (int)$response[1];

                case "EXPECTED_CRLF":
                    throw new ExpectedCrlfException;

                case "JOB_TOO_BIG":
                    throw new JobTooBigException;

                case "DRAINING":
                    throw new DrainingException;

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function reserve(?int $timeout = null): Future
    {
        $payload = $timeout === null ? "reserve\r\n" : "reserve-with-timeout $timeout\r\n";

        return $this->send($payload, function (array $response): array {
            [$type] = $response;

            switch ($type) {
                case "DEADLINE_SOON":
                    throw new DeadlineSoonException;

                case "TIMED_OUT":
                    throw new TimedOutException;

                case "RESERVED":
                    return [$response[1], $response[2]];

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function delete(int $id): Future
    {
        $payload = "delete $id\r\n";

        return $this->send($payload, function (array $response): bool {
            [$type] = $response;

            switch ($type) {
                case "DELETED":
                    return true;

                case "NOT_FOUND":
                    return false;

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function release(int $id, int $delay = 0, int $priority = 0): Future
    {
        $payload = "release $id $priority $delay\r\n";

        return $this->send($payload, function (array $response): string {
            [$type] = $response;

            switch ($type) {
                case "BURIED":
                case "RELEASED":
                case "NOT_FOUND":
                    return $type;

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function bury(int $id, int $priority = 0): Future
    {
        $payload = "bury $id $priority\r\n";

        return $this->send($payload, function (array $response): bool {
            [$type] = $response;

            switch ($type) {
                case "BURIED":
                    return true;

                case "NOT_FOUND":
                    return false;

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function kickJob(int $id): Future
    {
        $payload = "kick-job $id\r\n";

        return $this->send($payload, function (array $response): bool {
            [$type] = $response;

            switch ($type) {
                case "KICKED":
                    return true;

                case "NOT_FOUND":
                    return false;

                default:
                    throw new BeanstalkException("Unknown response: $type");
            }
        });
    }

    public function kick(int $count): Future
    {
        $payload = "kick $count\r\n";

        return $this->send($payload, function (array $response): int {
            [$type] = $response;

            switch ($type) {
                case "KICKED":
                    return (int)$response[1];

                default:
                    throw new BeanstalkException("Unknown response: $type");
            }
        });
    }

    public function touch(int $id): Future
    {
        $payload = "touch $id\r\n";

        return $this->send($payload, function (array $response): bool {
            [$type] = $response;

            switch ($type) {
                case "TOUCHED":
                    return true;

                case "NOT_FOUND":
                    return false;

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function watch(string $tube): Future
    {
        $payload = "watch $tube\r\n";

        return $this->send($payload, function (array $response): int {
            if ($response[0] !== "WATCHING") {
                throw new BeanstalkException("Unknown response: " . $response[0]);
            }

            return (int)$response[1];
        });
    }

    public function ignore(string $tube): Future
    {
        $payload = "ignore $tube\r\n";

        return $this->send($payload, function (array $response): int {
            [$type] = $response;

            switch ($type) {
                case "WATCHING":
                    return (int)$response[1];

                case "NOT_IGNORED":
                    throw new NotIgnoredException;

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function quit(): void
    {
        $this->send("quit\r\n");
    }

    public function getJobStats(int $id): Future
    {
        $payload = "stats-job $id\r\n";

        return $this->send($payload, function (array $response) use ($id): Job {
            [$type] = $response;

            switch ($type) {
                case "OK":
                    return new Job(Yaml::parse($response[1]));

                case "NOT_FOUND":
                    throw new NotFoundException("Job with $id is not found");

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function getTubeStats(string $tube): Future
    {
        $payload = "stats-tube $tube\r\n";

        return $this->send($payload, function (array $response) use ($tube): Tube {
            [$type] = $response;

            switch ($type) {
                case "OK":
                    return new Tube(Yaml::parse($response[1]));

                case "NOT_FOUND":
                    throw new NotFoundException("Tube $tube is not found");

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function getSystemStats(): Future
    {
        $payload = "stats\r\n";

        return $this->send($payload, function (array $response): System {
            if ($response[0] !== "OK") {
                throw new BeanstalkException("Unknown response: " . $response[0]);
            }

            return new System(Yaml::parse($response[1]));
        });
    }

    public function listTubes(): Future
    {
        $payload = "list-tubes\r\n";

        return $this->send($payload, function (array $response): array {
            [$type] = $response;

            switch ($type) {
                case "OK":
                    return Yaml::parse($response[1]);

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function listWatchedTubes(): Future
    {
        $payload = "list-tubes-watched\r\n";

        return $this->send($payload, function (array $response): array {
            [$type] = $response;

            switch ($type) {
                case "OK":
                    return Yaml::parse($response[1]);

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function getUsedTube(): Future
    {
        $payload = "list-tube-used\r\n";

        return $this->send($payload, function (array $response): string {
            [$type] = $response;

            switch ($type) {
                case "USING":
                    return $response[1];

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function peek(int $id): Future
    {
        $payload = "peek $id\r\n";

        return $this->send($payload, function (array $response) use ($id): string {
            [$type] = $response;

            switch ($type) {
                case "FOUND":
                    return $response[2];

                case "NOT_FOUND":
                    throw new NotFoundException("Job with $id is not found");

                default:
                    throw new BeanstalkException("Unknown response: " . $type);
            }
        });
    }

    public function peekReady(bool $peekId = false): Future
    {
        return $this->peekInState('ready', $peekId);
    }

    public function peekDelayed(bool $peekId = false): Future
    {
        return $this->peekInState('delayed', $peekId);
    }

    public function peekBuried(bool $peekId = false): Future
    {
        return $this->peekInState('buried', $peekId);
    }

    private function peekInState(string $state, bool $peekId = false): Future
    {
        $payload = "peek-$state\r\n";

        return $this->send(
            $payload,
            function (array $response) use ($state, $peekId): string {
                [$type] = $response;

                switch ($type) {
                    case "FOUND":
                        if ($peekId) {
                            return $response[1];
                        }
                        return $response[2];

                    case "NOT_FOUND":
                        throw new NotFoundException("No Job in $state state");

                    default:
                        throw new BeanstalkException("Unknown response: " . $type);
                }
            }
        );
    }

    private function failAllDeferreds(Throwable $error): void
    {
        while ($this->deferreds) {
            /** @var DeferredFuture $deferred */
            $deferred = array_shift($this->deferreds);
            $deferred->error($error);
        }
    }
}
