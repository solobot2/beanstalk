<?php

namespace Amp\Beanstalk\Test;

use Amp\Beanstalk\BeanstalkClient;
use Amp\Beanstalk\NotFoundException;
use Amp\Beanstalk\Stats\Job;
use Amp\Beanstalk\Stats\System;

use PHPUnit\Framework\TestCase;

use function getenv;

class IntegrationTest extends TestCase
{
    /** @var BeanstalkClient */
    private $beanstalk = null;

    private $tubeName = 'tests';

    public function setUp(): void
    {
        if (!getenv("AMP_TEST_BEANSTALK_INTEGRATION") && !getenv("TRAVIS")) {
             $this->markTestSkipped("You need to set AMP_TEST_BEANSTALK_INTEGRATION=1 in order to run the integration tests.");
        }

        $this->beanstalk?->quit();

        $this->beanstalk = new BeanstalkClient("tcp://127.0.0.1:11300");
        $this->beanstalk->use($this->tubeName);
        $this->beanstalk->watch($this->tubeName);

        /** @var System $stats */

        try {
            do {
                $jobId = $this->beanstalk->peekReady(true)->await();
                $this->beanstalk->delete($jobId)->await();
            } while (true);
        } catch (NotFoundException) {
        }
        try {
            do {
                $jobId = $this->beanstalk->peekDelayed(true)->await();
                $this->beanstalk->delete($jobId)->await();
            } while (true);
        } catch (NotFoundException) {
        }

        try {
            do {
                $jobId = $this->beanstalk->peekBuried(true)->await();
                $this->beanstalk->delete($jobId)->await();
            } while (true);
        } catch (NotFoundException) {
        }
    }

    public function testPut()
    {
        /** @var System $statsBefore */
        $statsBefore = $this->beanstalk->getSystemStats()->await();

        $jobId = $this->beanstalk->put("hi")->await();

        $this->assertIsInt($jobId);

        /** @var Job $jobStats */
        $jobStats = $this->beanstalk->getJobStats($jobId)->await();

        $this->assertSame($jobId, $jobStats->id);
        $this->assertSame(0, $jobStats->priority);
        $this->assertSame(0, $jobStats->delay);

        /** @var System $statsAfter */
        $statsAfter = $this->beanstalk->getSystemStats()->await();

        $this->assertSame($statsBefore->cmdPut + 1, $statsAfter->cmdPut);
    }

    public function testPeek()
    {
        $jobId = $this->beanstalk->put('I am ready')->await();
        $this->assertIsInt($jobId);

        $peekedJob = $this->beanstalk->peek($jobId)->await();
        $this->assertEquals('I am ready', $peekedJob);

        $peekedJob = $this->beanstalk->peekReady()->await();
        $this->assertEquals('I am ready', $peekedJob);

        [$jobId] = $this->beanstalk->reserve()->await();
        $buried = $this->beanstalk->bury($jobId)->await();
        $this->assertEquals(1, $buried);
        $peekedJob = $this->beanstalk->peekBuried()->await();
        $this->assertEquals('I am ready', $peekedJob);

        $jobId = $this->beanstalk->put('I am delayed', 60, 60)->await();
        $peekedJob = $this->beanstalk->peekDelayed()->await();
        $this->assertEquals('I am delayed', $peekedJob);
    }

    public function testKickJob()
    {
        $jobId = $this->beanstalk->put("hi")->await();
        $this->assertIsInt($jobId);

        $kicked = $this->beanstalk->kickJob($jobId)->await();
        $this->assertFalse($kicked);

        [$jobId,] = $this->beanstalk->reserve()->await();
        $buried = $this->beanstalk->bury($jobId)->await();
        $this->assertEquals(1, $buried);
        /** @var Job $jobStats */
        $jobStats = $this->beanstalk->getJobStats($jobId)->await();
        $this->assertEquals('buried', $jobStats->state);

        $kicked = $this->beanstalk->kickJob($jobId)->await();
        $this->assertTrue($kicked);
    }

    public function testKick()
    {
        for ($i = 0; $i < 10; $i++) {
            $this->beanstalk->put("Job $i")->await();
        }
        for ($i = 0; $i < 8; $i++) {
            [$jobId,] = $this->beanstalk->reserve()->await();
            $buried = $this->beanstalk->bury($jobId)->await();
            $this->assertEquals(1, $buried);
        }

        $kicked = $this->beanstalk->kick(4)->await();
        $this->assertEquals(4, $kicked);

        $kicked = $this->beanstalk->kick(10)->await();
        $this->assertEquals(4, $kicked);

        $kicked = $this->beanstalk->kick(1)->await();
        $this->assertEquals(0, $kicked);
    }

    public function testReservedJobShouldHaveTheSamePayloadAsThePutPayload()
    {
        $jobId = $this->beanstalk->put(str_repeat('*', 65535))->await();

        [$reservedJobId, $reservedJobPayload] = $this->beanstalk->reserve()->await();

        $this->assertSame($jobId, $reservedJobId);
        $this->assertSame(65535, strlen($reservedJobPayload));
    }
}
