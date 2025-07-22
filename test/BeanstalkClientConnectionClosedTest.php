<?php

namespace Amp\Beanstalk\Test;

use Amp\Beanstalk\BeanstalkClient;
use Amp\Beanstalk\ConnectionClosedException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Socket\ConnectContext;


use Amp\PHPUnit\AsyncTestCase;

use Amp\Socket\Socket;
use Amp\Socket\SocketException;

use function Amp\async;
use function Amp\delay;
use function Amp\Future\awaitAll;
use function Amp\Socket\connect;
use function Amp\Socket\listen;

class BeanstalkClientConnectionClosedTest extends AsyncTestCase {
    /** @var Socket */
    private $server;

    /**
     * @throws SocketException
     */
    public function setUp(): void
    {
        parent::setUp();
        $this->server = listen("tcp://127.0.0.1:0", );
    }

    public function tearDown(): void
    {
        parent::tearDown();
        $this->server->close();
    }

    /**
     * @dataProvider dataProviderReserve
     *
     * @param $reserveTimeout int|null Seconds
     * @param $connectionCloseTimeout int Milliseconds
     * @param $testFailTimeout int Milliseconds
     */
    public function testReserve(?int $reserveTimeout, int $connectionCloseTimeout, int $testFailTimeout) {
        $beanstalk = new BeanstalkClient("tcp://". $this->server->getAddress()->toString());

        $connectionClosePromise = async(function ($connectionCloseTimeout) {
            delay($connectionCloseTimeout);
            $this->server->close();
        },$connectionCloseTimeout);
        $this->setTimeout($testFailTimeout);
        $this->expectException(ConnectionClosedException::class);
       Future\await([
            $beanstalk->reserve($reserveTimeout),
            $connectionClosePromise
        ]);
    }

    public function dataProviderReserve(): array {
        return [
            "no timeout" => [null, 2, 3],
            "one second timeout" => [1, 2, 5],
        ];
    }
}
