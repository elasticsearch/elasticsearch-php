<?php

namespace Iprice\Elasticsearch\Common\Exceptions\Curl;

use Iprice\Elasticsearch\Common\Exceptions\ElasticsearchException;
use Iprice\Elasticsearch\Common\Exceptions\TransportException;

/**
 * Class CouldNotResolveHostException
 *
 * @category Elasticsearch
 * @package  Iprice\Elasticsearch\Common\Exceptions\Curl
 * @author   Zachary Tong <zach@elastic.co>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache2
 * @link     http://elastic.co
  */
class CouldNotResolveHostException extends TransportException implements ElasticsearchException
{
}
