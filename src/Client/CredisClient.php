<?php

declare(strict_types=1);

namespace Resque\Client;

use Credis_Client;

/**
 * Very light wrapper around Credis client that allows it to be used with php-resque
 *
 * @method bool sismember($key, $value)
 */
class CredisClient extends Credis_Client
{
    /**
     * Whether the client is connected to the server
     *
     * Overridden to provide access to the protected $connected variable.
     *
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * Disconnects the client
     */
    public function disconnect(): void
    {
        $this->close();
    }

    /**
     * Alias to exec() for pipeline compatibility with Predis
     */
    public function execute(): void
    {
        $this->__call('exec', []);
    }
}
