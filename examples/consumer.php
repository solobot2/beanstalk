<?php

require __DIR__ . '/../vendor/autoload.php';

use Amp\Beanstalk\BeanstalkClient;
use Revolt\EventLoop;

EventLoop::queue(function () {
    $beanstalk = new BeanstalkClient("tcp://127.0.0.1:11300");
    $beanstalk->watch('foobar')->await();

    while (list($jobId, $payload) = $beanstalk->reserve()->await()) {
        echo "Job id: $jobId\n";
        echo "Payload: $payload\n";

        $beanstalk->delete($jobId)->await();
    }
});
