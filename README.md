# beanstalk

[![Build Status](https://img.shields.io/travis/amphp/beanstalk/master.svg?style=flat-square)](https://travis-ci.org/amphp/beanstalk)
[![CoverageStatus](https://img.shields.io/coveralls/amphp/beanstalk/master.svg?style=flat-square)](https://coveralls.io/github/amphp/beanstalk?branch=master)
![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)

`amphp/beanstalk` is an asynchronous [Beanstalk](http://kr.github.io/beanstalkd/) client for PHP based on Amp.

## Installation

```
composer require amphp/beanstalk
```

## Examples

All command methods return `Amp\Future`. Await the returned future with `->await()`.

```php
<?php

require __DIR__ . '/../vendor/autoload.php';

use Amp\Beanstalk\BeanstalkClient;

$beanstalk = new BeanstalkClient("tcp://127.0.0.1:11300");
$beanstalk->use('sometube')->await();

$payload = json_encode([
    "job" => bin2hex(random_bytes(16)),
    "type" => "compress-image",
    "path" => "/path/to/image.png"
]);

$jobId = $beanstalk->put($payload)->await();

echo "Inserted job id: $jobId\n";

$beanstalk->quit();
```
## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
