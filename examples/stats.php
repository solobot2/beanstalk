<?php

require __DIR__ . '/../vendor/autoload.php';

use Amp\Beanstalk\BeanstalkClient;
use Amp\Beanstalk\Stats\System;


Revolt\EventLoop::queue(function () {
    $beanstalk = new BeanstalkClient("tcp://127.0.0.1:11300");

    /**
     * @var System $systemStats
     */
    $systemStats = $beanstalk->getSystemStats()->await();
    echo "Active connections: {$systemStats->currentConnections}\n";
    echo "Jobs ready: {$systemStats->currentJobsReady}\n";

    $beanstalk->quit();
});
