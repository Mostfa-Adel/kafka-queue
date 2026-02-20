<?php
namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected $consumer;
    protected $producer;

    public function __construct(\RdKafka\KafkaConsumer $consumer, \RdKafka\Producer $producer)
    {
        $this->consumer = $consumer;
        $this->producer = $producer;
    }

    public function push($job, $data = '', $queue = null)
    {
        $this->producer->newTopic($queue ?? env('KAFKA_DEFAULT_TOPIC'))->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(10000);
    }

    public function pop($queue = null)
    {
        try {
            $this->consumer->subscribe([$queue]);
            
            $message = $this->consumer->consume(120*1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more messages; will wait for more\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump("Timed out\n");
                    break;
                default:
                    var_dump("Error: {$message->errstr()}\n");
                    break;
            }
        } catch (\Exception $e) {
            \Log::error("Kafka consumer error: {$e->getMessage()}");
            var_dump("Error: {$e->getMessage()}\n");
        }
        
    }
    public function size($queue = null)
    {
    }
    public function pushRaw($payload, $queue = null, array $options = [])
    {
    }
    public function later($delay, $job, $data = '', $queue = null)
    {
    }
}