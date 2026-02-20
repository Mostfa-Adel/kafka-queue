<?php

namespace App;

use Illuminate\Support\ServiceProvider;
use App\Connectors\KafkaConnector;

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
