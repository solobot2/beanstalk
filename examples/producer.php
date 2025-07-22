<?php

require __DIR__ . '/../vendor/autoload.php';

use Amp\Beanstalk\BeanstalkClient;

Revolt\EventLoop::queue(function () {
    $beanstalk = new BeanstalkClient("tcp://127.0.0.1:11300");
    $beanstalk->use('foobar')->await();

    $payload = json_encode([
        "job" => bin2hex(random_bytes(16)),
        "type" => "compress-image",
        "path" => "/path/to/image.png"
    ]);

    $jobId =  $beanstalk->put($payload)->await();

    echo "Inserted job id: $jobId\n";

    $beanstalk->quit();
});
