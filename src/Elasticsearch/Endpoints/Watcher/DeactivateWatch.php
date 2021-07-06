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

namespace Elasticsearch\Endpoints\Watcher;

use Elasticsearch\Common\Exceptions\RuntimeException;
use Elasticsearch\Endpoints\AbstractEndpoint;

/**
 * Class DeactivateWatch
 * Elasticsearch API name xpack.watcher.deactivate_watch
 *
 * NOTE: this file is autogenerated using util/GenerateEndpoints.php
 * and Elasticsearch 6.8.16 (1f62092)
 */
class DeactivateWatch extends AbstractEndpoint
{
    protected $watch_id;

    public function getURI(): string
    {
        if (isset($this->watch_id) !== true) {
            throw new RuntimeException(
                'watch_id is required for Deactivate_watch'
            );
        }
        $watch_id = $this->watch_id;

        return "/_xpack/watcher/watch/$watch_id/_deactivate";
    }

    public function getParamWhitelist(): array
    {
        return [
            'master_timeout'
        ];
    }

    public function getMethod(): string
    {
        return 'PUT';
    }

    public function setWatchId($watch_id): DeactivateWatch
    {
        if (isset($watch_id) !== true) {
            return $this;
        }
        $this->watch_id = $watch_id;

        return $this;
    }
}
