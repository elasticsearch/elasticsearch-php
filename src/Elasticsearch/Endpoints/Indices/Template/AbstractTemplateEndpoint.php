<?php

namespace Elasticsearch\Endpoints\Indices\Template;

use Elasticsearch\Endpoints\AbstractEndpoint;

/**
 * Class AbstractTemplateEndpoint
 *
 * @category Elasticsearch
 * @package  Elasticsearch\Endpoints\Indices\Template
 * @author   Zachary Tong <zach@elastic.co>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache2
 * @link     http://elastic.co
 */
abstract class AbstractTemplateEndpoint extends AbstractEndpoint
{
    /** @var  string : the name of the template */
    protected $name = null;

    /**
     * @param $name
     *
     * @return $this
     */
    public function setName($name)
    {
        if (isset($name) !== true) {
            return $this;
        }

        $this->name = $name;

        return $this;
    }
}
