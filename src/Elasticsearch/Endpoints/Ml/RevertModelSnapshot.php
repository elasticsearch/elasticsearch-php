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

namespace Elasticsearch\Endpoints\Ml;

use Elasticsearch\Common\Exceptions\RuntimeException;
use Elasticsearch\Endpoints\AbstractEndpoint;

/**
 * Class RevertModelSnapshot
 * Elasticsearch API name ml.revert_model_snapshot
 *
 * NOTE: this file is autogenerated using util/GenerateEndpoints.php
 * and Elasticsearch 8.0.0-SNAPSHOT (ca2fb5c7ee55464068a6581480e9db6ebe569e6d)
 */
class RevertModelSnapshot extends AbstractEndpoint
{
    protected $job_id;
    protected $snapshot_id;

    public function getURI(): string
    {
        $job_id = $this->job_id ?? null;
        $snapshot_id = $this->snapshot_id ?? null;

        if (isset($job_id) && isset($snapshot_id)) {
            return "/_ml/anomaly_detectors/$job_id/model_snapshots/$snapshot_id/_revert";
        }
        throw new RuntimeException('Missing parameter for the endpoint ml.revert_model_snapshot');
    }

    public function getParamWhitelist(): array
    {
        return [
            'delete_intervening_results'
        ];
    }

    public function getMethod(): string
    {
        return 'POST';
    }

    public function setBody($body): RevertModelSnapshot
    {
        if (isset($body) !== true) {
            return $this;
        }
        $this->body = $body;

        return $this;
    }

    public function setJobId($job_id): RevertModelSnapshot
    {
        if (isset($job_id) !== true) {
            return $this;
        }
        $this->job_id = $job_id;

        return $this;
    }

    public function setSnapshotId($snapshot_id): RevertModelSnapshot
    {
        if (isset($snapshot_id) !== true) {
            return $this;
        }
        $this->snapshot_id = $snapshot_id;

        return $this;
    }
}
