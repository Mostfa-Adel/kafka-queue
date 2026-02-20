<?php

namespace Kafka;

use Illuminate\Support\ServiceProvider;

class KafkaServiceProvider extends ServiceProvider
{

    /**
     * Bootstrap services.
     */
    public function boot(): void
    {
        $this->app['queue']->addConnector('kafka', function () {
            return $this->app->make(KafkaConnector::class);
        });
    }
}
