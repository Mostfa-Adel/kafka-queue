<?php

namespace App;

use Illuminate\Queue\Connectors\ConnectorInterface;
use App\Queues\KafkaQueue;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $conf = new \RdKafka\Conf();
        $conf->set('group.id', $config['group_id']);
        $conf->set('metadata.broker.list', $config['broker_list']);
        $producer = new \RdKafka\Producer($conf);
        $consumer = new \RdKafka\KafkaConsumer($conf);
        return new KafkaQueue($consumer, $producer);
    }
}