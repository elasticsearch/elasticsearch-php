<?php
/**
 * Elasticsearch PHP Client
 *
 * @link      https://github.com/elastic/elasticsearch-php
 * @copyright Copyright (c) Elasticsearch B.V (https://www.elastic.co)
 * @license   https://opensource.org/licenses/MIT MIT License
 *
 * Licensed to Elasticsearch B.V under one or more agreements.
 * Elasticsearch B.V licenses this file to you under the MIT License.
 * See the LICENSE file in the project root for more information.
 */
declare(strict_types = 1);

namespace Elastic\Elasticsearch;

use Elastic\Elasticsearch\Response\Elasticsearch;
use Elastic\Elasticsearch\Traits\ClientEndpointsTrait;
use Elastic\Elasticsearch\Traits\EndpointTrait;
use Elastic\Elasticsearch\Traits\NamespaceTrait;
use Elastic\Elasticsearch\Transport\AsyncOnSuccess;
use Elastic\Elasticsearch\Transport\AsyncOnSuccessNoException;
use Elastic\Transport\Transport;
use Psr\Http\Message\RequestInterface;
use Psr\Log\LoggerInterface;

final class Client implements ClientInterface
{
    const CLIENT_NAME = 'es';
    const VERSION = '8.x';

    use ClientEndpointsTrait;
    use EndpointTrait;
    use NamespaceTrait;
    
    protected Transport $transport;
    protected LoggerInterface $logger;

    /**
     * Specify is the request is asyncronous
     */
    protected bool $async = false;
    
    /**
     * Enable or disable the x-elastic-meta-header
     */
    protected bool $elasticMetaHeader = true;

    /**
     * Enable or disable the response Exception
     */
    protected bool $responseException = true;

    /**
     * The endpoint namespace storage 
     */
    protected array $namespace;

    public function __construct(
        Transport $transport, 
        LoggerInterface $logger
    ) {
        $this->transport = $transport;
        $this->logger = $logger;       
        
        $this->defaultTransportSettings($this->transport);
    }

    public function getTransport(): Transport
    {
        return $this->transport;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * Set the default settings for Elasticsearch
     */
    protected function defaultTransportSettings(Transport $transport): void
    {
        $transport->setUserAgent('elasticsearch-php', self::VERSION);
    }

    /**
     * @inheritDoc
     */
    public function setAsync(bool $async): self
    {
        $this->async = $async;
        return $this;
    }

    /**
     * @inheritDoc
     */
    public function getAsync(): bool
    {
        return $this->async;
    }

    /**
     * @inheritDoc
     */
    public function setElasticMetaHeader(bool $active): self
    {
        $this->elasticMetaHeader = $active;
        return $this;
    }

    /**
     * @inheritDoc
     */
    public function getElasticMetaHeader(): bool
    {
        return $this->elasticMetaHeader;
    }

    /**
     * @inheritDoc
     */
    public function setResponseException(bool $active): self
    {
        $this->responseException = $active;
        return $this;
    }

    /**
     * @inheritDoc
     */
    public function getResponseException(): bool
    {
        return $this->responseException;
    }

    /**
     * @inheritDoc
     */
    public function sendRequest(RequestInterface $request)
    {   
        // If async returns a Promise
        if ($this->getAsync()) {
            if ($this->getElasticMetaHeader()) {
                $this->transport->setElasticMetaHeader(Client::CLIENT_NAME, Client::VERSION, true);
            }
            $this->transport->setAsyncOnSuccess(
                $request->getMethod() === 'HEAD'
                    ? new AsyncOnSuccessNoException
                    : ($this->getResponseException() ? new AsyncOnSuccess : new AsyncOnSuccessNoException)
            );
            return $this->transport->sendAsyncRequest($request);
        }     

        if ($this->getElasticMetaHeader()) {
            $this->transport->setElasticMetaHeader(Client::CLIENT_NAME, Client::VERSION, false);
        }
        $start = microtime(true);
        $response = $this->transport->sendRequest($request);
        $this->logger->info(sprintf("Response time in %.3f sec", microtime(true) - $start));       

        $result = new Elasticsearch;
        $result->setResponse($response, $request->getMethod() === 'HEAD' ? false : $this->getResponseException());
        return $result;
    }
}