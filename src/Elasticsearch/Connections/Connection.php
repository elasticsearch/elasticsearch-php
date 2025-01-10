<?php
/**
 * Elasticsearch PHP client
 *
 * @link      https://github.com/elastic/elasticsearch-php/
 * @copyright Copyright (c) Elasticsearch B.V (https://www.elastic.co)
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @license   https://www.gnu.org/licenses/lgpl-2.1.html GNU Lesser General Public License, Version 2.1 
 * 
 * Licensed to Elasticsearch B.V under one or more agreements.
 * Elasticsearch B.V licenses this file to you under the Apache 2.0 License or
 * the GNU Lesser General Public License, Version 2.1, at your option.
 * See the LICENSE file in the project root for more information.
 */


declare(strict_types = 1);

namespace Elasticsearch\Connections;

use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\AlreadyExpiredException;
use Elasticsearch\Common\Exceptions\BadRequest400Exception;
use Elasticsearch\Common\Exceptions\Conflict409Exception;
use Elasticsearch\Common\Exceptions\Curl\CouldNotConnectToHost;
use Elasticsearch\Common\Exceptions\Curl\CouldNotResolveHostException;
use Elasticsearch\Common\Exceptions\Curl\OperationTimeoutException;
use Elasticsearch\Common\Exceptions\Forbidden403Exception;
use Elasticsearch\Common\Exceptions\MaxRetriesException;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Elasticsearch\Common\Exceptions\NoDocumentsToGetException;
use Elasticsearch\Common\Exceptions\NoShardAvailableException;
use Elasticsearch\Common\Exceptions\RequestTimeout408Exception;
use Elasticsearch\Common\Exceptions\RoutingMissingException;
use Elasticsearch\Common\Exceptions\ScriptLangNotSupportedException;
use Elasticsearch\Common\Exceptions\ServerErrorResponseException;
use Elasticsearch\Common\Exceptions\TransportException;
use Elasticsearch\Serializers\SerializerInterface;
use Elasticsearch\Transport;
use GuzzleHttp\Ring\Core;
use GuzzleHttp\Ring\Exception\ConnectException;
use GuzzleHttp\Ring\Exception\RingException;
use Psr\Log\LoggerInterface;

/**
 * Class AbstractConnection
 *
 */
class Connection implements ConnectionInterface
{
    /** @var  callable */
    protected $handler;

    /** @var SerializerInterface */
    protected $serializer;

    /**
     * @var string
     */
    protected $transportSchema = 'http';    // TODO depreciate this default

    /**
     * @var string
     */
    protected $host;

    /**
     * @var string|null
     */
    protected $path;

    /**
     * @var LoggerInterface
     */
    protected $log;

    /**
     * @var LoggerInterface
     */
    protected $trace;

    /**
     * @var array
     */
    protected $connectionParams;

    /** @var  array */
    protected $headers = [];

    /** @var bool  */
    protected $isAlive = false;

    /** @var float  */
    private $pingTimeout = 1;    //TODO expose this

    /** @var int  */
    private $lastPing = 0;

    /** @var int  */
    private $failedPings = 0;

    private $lastRequest = array();

    /**
     * @param callable $handler
     * @param array $hostDetails
     * @param array $connectionParams Array of connection-specific parameters
     * @param \Elasticsearch\Serializers\SerializerInterface $serializer
     * @param \Psr\Log\LoggerInterface $log              Logger object
     * @param \Psr\Log\LoggerInterface $trace
     */
    public function __construct(
        $handler,
        $hostDetails,
        $connectionParams,
        SerializerInterface $serializer,
        LoggerInterface $log,
        LoggerInterface $trace
    ) {

        if (isset($hostDetails['port']) !== true) {
            $hostDetails['port'] = 9200;
        }

        if (isset($hostDetails['scheme'])) {
            $this->transportSchema = $hostDetails['scheme'];
        }

        if (isset($hostDetails['user']) && isset($hostDetails['pass'])) {
            $connectionParams['client']['curl'][CURLOPT_HTTPAUTH] = CURLAUTH_BASIC;
            $connectionParams['client']['curl'][CURLOPT_USERPWD] = $hostDetails['user'].':'.$hostDetails['pass'];
        }

        if (isset($connectionParams['client']['headers']) === true) {
            $this->headers = $connectionParams['client']['headers'];
            unset($connectionParams['client']['headers']);
        }

        // Add the User-Agent using the format: <client-repo-name>/<client-version> (metadata-values)
        $this->headers['User-Agent'] = [sprintf(
            "elasticsearch-php/%s (%s %s, PHP %s)",
            Client::VERSION,
            php_uname("s"),
            php_uname("r"),
            phpversion()
        )];

        // Add x-elastic-client-meta header, if enabled
        if (isset($connectionParams['client']['x-elastic-client-meta']) && $connectionParams['client']['x-elastic-client-meta']) {
            $this->headers['x-elastic-client-meta'] = [$this->getElasticMetaHeader($connectionParams)];
        }

        $host = $hostDetails['host'].':'.$hostDetails['port'];
        $path = null;
        if (isset($hostDetails['path']) === true) {
            $path = $hostDetails['path'];
        }
        $this->host             = $host;
        $this->path             = $path;
        $this->log              = $log;
        $this->trace            = $trace;
        $this->connectionParams = $connectionParams;
        $this->serializer       = $serializer;

        $this->handler = $this->wrapHandler($handler);
    }

    /**
     * @param string $method
     * @param string $uri
     * @param array $params
     * @param null $body
     * @param array $options
     * @param \Elasticsearch\Transport $transport
     * @return mixed
     */
    public function performRequest($method, $uri, $params = null, $body = null, $options = [], ?Transport $transport = null)
    {
        if ($body !== null) {
            $body = $this->serializer->serialize($body);
        }

        $headers = $this->headers;
        if (isset($options['client']['headers']) && is_array($options['client']['headers'])) {
            $headers = array_merge($this->headers, $options['client']['headers']);
        }

        $request = [
            'http_method' => $method,
            'scheme'      => $this->transportSchema,
            'uri'         => $this->getURI($uri, $params),
            'body'        => $body,
            'headers'     => array_merge(['Host' => [$this->host]], $headers)
        ];

        $request = array_replace_recursive($request, $this->connectionParams, $options);

        // RingPHP does not like if client is empty
        if (empty($request['client'])) {
            unset($request['client']);
        }

        $handler = $this->handler;
        $future = $handler($request, $this, $transport, $options);

        return $future;
    }

    /** @return array */
    public function getHeaders()
    {
        return $this->headers;
    }

    /** @return string */
    public function getTransportSchema()
    {
        return $this->transportSchema;
    }

    /** @return array */
    public function getLastRequestInfo()
    {
        return $this->lastRequest;
    }

    private function wrapHandler(callable $handler)
    {
        return function (array $request, Connection $connection, ?Transport $transport = null, $options = []) use ($handler) {

            $this->lastRequest = [];
            $this->lastRequest['request'] = $request;

            // Send the request using the wrapped handler.
            $response =  Core::proxy($handler($request), function ($response) use ($connection, $transport, $request, $options) {

                $this->lastRequest['response'] = $response;

                if (isset($response['error']) === true) {
                    if ($response['error'] instanceof ConnectException || $response['error'] instanceof RingException) {
                        $this->log->warning("Curl exception encountered.");

                        $exception = $this->getCurlRetryException($request, $response);

                        $this->logRequestFail(
                            $request['http_method'],
                            $response['effective_url'],
                            $request['body'],
                            $request['headers'],
                            $response['status'],
                            $response['body'],
                            $response['transfer_stats']['total_time'],
                            $exception
                        );

                        $node = $connection->getHost();
                        $this->log->warning("Marking node $node dead.");
                        $connection->markDead();

                        // If the transport has not been set, we are inside a Ping or Sniff,
                        // so we don't want to retrigger retries anyway.
                        //
                        // TODO this could be handled better, but we are limited because connectionpools do not
                        // have access to Transport.  Architecturally, all of this needs to be refactored
                        if (isset($transport) === true) {
                            $transport->connectionPool->scheduleCheck();

                            $neverRetry = isset($request['client']['never_retry']) ? $request['client']['never_retry'] : false;
                            $shouldRetry = $transport->shouldRetry($request);
                            $shouldRetryText = ($shouldRetry) ? 'true' : 'false';

                            $this->log->warning("Retries left? $shouldRetryText");
                            if ($shouldRetry && !$neverRetry) {
                                return $transport->performRequest(
                                    $request['http_method'],
                                    $request['uri'],
                                    [],
                                    $request['body'],
                                    $options
                                );
                            }
                        }

                        $this->log->warning("Out of retries, throwing exception from $node");
                        // Only throw if we run out of retries
                        throw $exception;
                    } else {
                        // Something went seriously wrong, bail
                        $exception = new TransportException($response['error']->getMessage());
                        $this->logRequestFail(
                            $request['http_method'],
                            $response['effective_url'],
                            $request['body'],
                            $request['headers'],
                            $response['status'],
                            $response['body'],
                            $response['transfer_stats']['total_time'],
                            $exception
                        );
                        throw $exception;
                    }
                } else {
                    $connection->markAlive();

                    if (isset($response['body']) === true) {
                        $response['body'] = stream_get_contents($response['body']);
                        $this->lastRequest['response']['body'] = $response['body'];
                    }

                    if ($response['status'] >= 400 && $response['status'] < 500) {
                        $ignore = isset($request['client']['ignore']) ? $request['client']['ignore'] : [];
                        $this->process4xxError($request, $response, $ignore);
                    } elseif ($response['status'] >= 500) {
                        $ignore = isset($request['client']['ignore']) ? $request['client']['ignore'] : [];
                        $this->process5xxError($request, $response, $ignore);
                    }

                    // No error, deserialize
                    $response['body'] = $this->serializer->deserialize($response['body'], $response['transfer_stats']);
                }
                $this->logRequestSuccess(
                    $request['http_method'],
                    $response['effective_url'],
                    $request['body'],
                    $request['headers'],
                    $response['status'],
                    $response['body'],
                    $response['transfer_stats']['total_time']
                );

                return isset($request['client']['verbose']) && $request['client']['verbose'] === true ? $response : $response['body'];
            });

            return $response;
        };
    }

    /**
     * @param string $uri
     * @param array $params
     *
     * @return string
     */
    private function getURI(string $uri, ?array $params): string
    {
        if (isset($params) === true && !empty($params)) {
            $params = array_map(
                function ($value) {
                    if ($value === true) {
                        return 'true';
                    } elseif ($value === false) {
                        return 'false';
                    }
                    return $value;
                },
                $params
            );

            $uri .= '?' . http_build_query($params);
        }

        if ($this->path !== null) {
            $uri = $this->path . $uri;
        }

        return $uri ?? '';
    }

    /**
     * Log a successful request
     *
     * @param string $method
     * @param string $fullURI
     * @param string $body
     * @param array  $headers
     * @param string $statusCode
     * @param string $response
     * @param string $duration
     *
     * @return void
     */
    public function logRequestSuccess($method, $fullURI, $body, $headers, $statusCode, $response, $duration)
    {
        $this->log->debug('Request Body', array($body));
        $this->log->info(
            'Request Success:',
            array(
                'method'    => $method,
                'uri'       => $fullURI,
                'headers'   => $headers,
                'HTTP code' => $statusCode,
                'duration'  => $duration,
            )
        );
        $this->log->debug('Response', array($response));

        // Build the curl command for Trace.
        $curlCommand = $this->buildCurlCommand($method, $fullURI, $body);
        $this->trace->info($curlCommand);
        $this->trace->debug(
            'Response:',
            array(
                'response'  => $response,
                'method'    => $method,
                'uri'       => $fullURI,
                'HTTP code' => $statusCode,
                'duration'  => $duration,
            )
        );
    }

    /**
     * Log a a failed request
     *
     * @param string $method
     * @param string $fullURI
     * @param string $body
     * @param array $headers
     * @param null|string $statusCode
     * @param null|string $response
     * @param string $duration
     * @param \Exception $exception
     *
     * @return void
     */
    public function logRequestFail($method, $fullURI, $body, $headers, $statusCode, $response, $duration, \Exception $exception)
    {
        $this->log->debug('Request Body', array($body));
        $this->log->warning(
            'Request Failure:',
            array(
                'method'    => $method,
                'uri'       => $fullURI,
                'headers'   => $headers,
                'HTTP code' => $statusCode,
                'duration'  => $duration,
                'error'     => $exception->getMessage(),
            )
        );
        $this->log->warning('Response', array($response));

        // Build the curl command for Trace.
        $curlCommand = $this->buildCurlCommand($method, $fullURI, $body);
        $this->trace->info($curlCommand);
        $this->trace->debug(
            'Response:',
            array(
                'response'  => $response,
                'method'    => $method,
                'uri'       => $fullURI,
                'HTTP code' => $statusCode,
                'duration'  => $duration,
            )
        );
    }

    /**
     * @return bool
     */
    public function ping()
    {
        $options = [
            'client' => [
                'timeout' => $this->pingTimeout,
                'never_retry' => true,
                'verbose' => true
            ]
        ];
        try {
            $response = $this->performRequest('HEAD', '/', null, null, $options);
            $response = $response->wait();
        } catch (TransportException $exception) {
            $this->markDead();

            return false;
        }

        if ($response['status'] === 200) {
            $this->markAlive();

            return true;
        } else {
            $this->markDead();

            return false;
        }
    }

    /**
     * @return array
     */
    public function sniff()
    {
        $options = [
            'client' => [
                'timeout' => $this->pingTimeout,
                'never_retry' => true
            ]
        ];

        return $this->performRequest('GET', '/_nodes/', null, null, $options);
    }

    /**
     * @return bool
     */
    public function isAlive()
    {
        return $this->isAlive;
    }

    public function markAlive()
    {
        $this->failedPings = 0;
        $this->isAlive = true;
        $this->lastPing = time();
    }

    public function markDead()
    {
        $this->isAlive = false;
        $this->failedPings += 1;
        $this->lastPing = time();
    }

    /**
     * @return int
     */
    public function getLastPing()
    {
        return $this->lastPing;
    }

    /**
     * @return int
     */
    public function getPingFailures()
    {
        return $this->failedPings;
    }

    /**
     * @return string
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * @return null|string
     */
    public function getUserPass()
    {
        if (isset($this->connectionParams['client']['curl'][CURLOPT_USERPWD]) === true) {
            return $this->connectionParams['client']['curl'][CURLOPT_USERPWD];
        }
        return null;
    }

    /**
     * @return null|string
     */
    public function getPath()
    {
        return $this->path;
    }

    /**
     * @param array $request
     * @param array $response
     * @return \Elasticsearch\Common\Exceptions\Curl\CouldNotConnectToHost|\Elasticsearch\Common\Exceptions\Curl\CouldNotResolveHostException|\Elasticsearch\Common\Exceptions\Curl\OperationTimeoutException|\Elasticsearch\Common\Exceptions\MaxRetriesException
     */
    protected function getCurlRetryException($request, $response)
    {
        $exception = null;
        $message = $response['error']->getMessage();
        $exception = new MaxRetriesException($message);
        switch ($response['curl']['errno']) {
            case 6:
                $exception = new CouldNotResolveHostException($message, 0, $exception);
                break;
            case 7:
                $exception = new CouldNotConnectToHost($message, 0, $exception);
                break;
            case 28:
                $exception = new OperationTimeoutException($message, 0, $exception);
                break;
        }

        return $exception;
    }

    /**
     * Construct a string cURL command
     *
     * @param string $method HTTP method
     * @param string $uri    Full URI of request
     * @param string $body   Request body
     *
     * @return string
     */
    private function buildCurlCommand(string $method, string $uri, ?string $body): string
    {
        if (strpos($uri, '?') === false) {
            $uri .= '?pretty=true';
        } else {
            str_replace('?', '?pretty=true', $uri);
        }

        $curlCommand = 'curl -X' . strtoupper($method);
        $curlCommand .= " '" . $uri . "'";

        if (isset($body) === true && $body !== '') {
            $curlCommand .= " -d '" . $body . "'";
        }

        return $curlCommand;
    }

    /**
     * @param array $request
     * @param array $response
     * @param array $ignore
     * @throws \Elasticsearch\Common\Exceptions\AlreadyExpiredException|\Elasticsearch\Common\Exceptions\BadRequest400Exception|\Elasticsearch\Common\Exceptions\Conflict409Exception|\Elasticsearch\Common\Exceptions\Forbidden403Exception|\Elasticsearch\Common\Exceptions\Missing404Exception|\Elasticsearch\Common\Exceptions\ScriptLangNotSupportedException
     */
    private function process4xxError($request, $response, $ignore)
    {
        $statusCode = $response['status'];
        $responseBody = $response['body'];

        /** @var \Exception $exception */
        $exception = $this->tryDeserialize400Error($response);

        if (array_search($response['status'], $ignore) !== false) {
            return;
        }

        // if responseBody is not string, we convert it so it can be used as Exception message
        if (!is_string($responseBody)) {
            $responseBody = json_encode($responseBody);
        }

        if ($statusCode === 400 && strpos($responseBody, "AlreadyExpiredException") !== false) {
            $exception = new AlreadyExpiredException($responseBody, $statusCode);
        } elseif ($statusCode === 403) {
            $exception = new Forbidden403Exception($responseBody, $statusCode);
        } elseif ($statusCode === 404) {
            $exception = new Missing404Exception($responseBody, $statusCode);
        } elseif ($statusCode === 409) {
            $exception = new Conflict409Exception($responseBody, $statusCode);
        } elseif ($statusCode === 400 && strpos($responseBody, 'script_lang not supported') !== false) {
            $exception = new ScriptLangNotSupportedException($responseBody. $statusCode);
        } elseif ($statusCode === 408) {
            $exception = new RequestTimeout408Exception($responseBody, $statusCode);
        } else {
            $exception = new BadRequest400Exception($responseBody, $statusCode);
        }

        $this->logRequestFail(
            $request['http_method'],
            $response['effective_url'],
            $request['body'],
            $request['headers'],
            $response['status'],
            $response['body'],
            $response['transfer_stats']['total_time'],
            $exception
        );

        throw $exception;
    }

    /**
     * @param array $request
     * @param array $response
     * @param array $ignore
     * @throws \Elasticsearch\Common\Exceptions\NoDocumentsToGetException|\Elasticsearch\Common\Exceptions\NoShardAvailableException|\Elasticsearch\Common\Exceptions\RoutingMissingException|\Elasticsearch\Common\Exceptions\ServerErrorResponseException
     */
    private function process5xxError($request, $response, $ignore)
    {
        $statusCode = $response['status'];
        $responseBody = $response['body'];

        /** @var \Exception $exception */
        $exception = $this->tryDeserialize500Error($response);

        $exceptionText = "[$statusCode Server Exception] ".$exception->getMessage();
        $this->log->error($exceptionText);
        $this->log->error($exception->getTraceAsString());

        if (array_search($statusCode, $ignore) !== false) {
            return;
        }

        if ($statusCode === 500 && strpos($responseBody, "RoutingMissingException") !== false) {
            $exception = new RoutingMissingException($exception->getMessage(), $statusCode, $exception);
        } elseif ($statusCode === 500 && preg_match('/ActionRequestValidationException.+ no documents to get/', $responseBody) === 1) {
            $exception = new NoDocumentsToGetException($exception->getMessage(), $statusCode, $exception);
        } elseif ($statusCode === 500 && strpos($responseBody, 'NoShardAvailableActionException') !== false) {
            $exception = new NoShardAvailableException($exception->getMessage(), $statusCode, $exception);
        } else {
            $exception = new ServerErrorResponseException($responseBody, $statusCode);
        }

        $this->logRequestFail(
            $request['http_method'],
            $response['effective_url'],
            $request['body'],
            $request['headers'],
            $response['status'],
            $response['body'],
            $response['transfer_stats']['total_time'],
            $exception
        );

        throw $exception;
    }

    private function tryDeserialize400Error($response)
    {
        return $this->tryDeserializeError($response, 'Elasticsearch\Common\Exceptions\BadRequest400Exception');
    }

    private function tryDeserialize500Error($response)
    {
        return $this->tryDeserializeError($response, 'Elasticsearch\Common\Exceptions\ServerErrorResponseException');
    }

    private function tryDeserializeError($response, $errorClass)
    {
        $error = $this->serializer->deserialize($response['body'], $response['transfer_stats']);
        if (is_array($error) === true) {
            // 2.0 structured exceptions
            if (isset($error['error']['reason']) === true) {
                // Try to use root cause first (only grabs the first root cause)
                $root = $error['error']['root_cause'];
                if (isset($root) && isset($root[0])) {
                    $cause = $root[0]['reason'];
                    $type = $root[0]['type'];
                } else {
                    $cause = $error['error']['reason'];
                    $type = $error['error']['type'];
                }

                $original = new $errorClass($response['body'], $response['status']);

                return new $errorClass("$type: $cause", $response['status'], $original);
            } elseif (isset($error['error']) === true) {
                // <2.0 semi-structured exceptions
                $original = new $errorClass($response['body'], $response['status']);

                return new $errorClass($error['error'], $response['status'], $original);
            }

            // <2.0 "i just blew up" nonstructured exception
            // $error is an array but we don't know the format, reuse the response body instead
            return new $errorClass($response['body'], $response['status']);
        }

        // if responseBody is not string, we convert it so it can be used as Exception message
        $responseBody = $response['body'];
        if (!is_string($responseBody)) {
            $responseBody = json_encode($responseBody);
        }

        // <2.0 "i just blew up" nonstructured exception
        return new $errorClass($responseBody);
    }

    /**
     * Get the x-elastic-client-meta header
     */
    private function getElasticMetaHeader(array $connectionParams): string
    {
        $phpSemVersion = sprintf("%d.%d.%d", PHP_MAJOR_VERSION, PHP_MINOR_VERSION, PHP_RELEASE_VERSION);
        // Reduce the size in case of '-snapshot' version (using 'p' as pre-release)
        $clientVersion = str_replace('-snapshot', '-p', strtolower(Client::VERSION)); 
        $clientMeta = sprintf(
            "es=%s,php=%s,t=%s,a=%d",
            $clientVersion,
            $phpSemVersion,
            $clientVersion,
            isset($connectionParams['client']['future']) && $connectionParams['client']['future'] === 'lazy' ? 1 : 0
        );
        if (function_exists('curl_version')) {
            $curlVersion = curl_version();
            if (isset($curlVersion['version'])) {
                $clientMeta .= sprintf(",cu=%s", $curlVersion['version']); // cu = curl library
            }
        }
        return $clientMeta;
    }
}
