<?php

namespace Elasticsearch\Namespaces;

/**
 * Class XpackNamespace
 *
 * @category Elasticsearch
 * @package  Elasticsearch\Namespaces
 * @author   Augustin Husson <husson.augustin@gmail.com>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache2
 * @link     http://elastic.co
 */
class XpackNamespace extends AbstractNamespace
{

    /**
     * $param['categories']    = (list) A comma-separated list of the information categories to include in the response. For example, build,license,features
     *       ['human']        = (boolean) Defines whether additional human-readable information is included in the response. In particular, it adds descriptions and a tag line. The default value is true.
     * @param array $params Associative array of parameters
     * @return array
     */
    public function get($params = array())
    {
        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Xpack\Get $endpoint */
        $endpoint = $endpointBuilder('Xpack\Get');

        $endpoint->setParams($params);

        return $this->performRequest($endpoint);
    }

    /**
     * $params['username'] = (string) An identifier for the user. If you omit this parameter, it retrieves information about all users.
     * @param array $params Associative array of parameters
     * @return array
     */
    public function getUser($params = array())
    {
        $username = $this->extractArgument($params, 'username');

        /** @var callback $endpointBuilder */
        $endpointBuilder = $this->endpoints;

        /** @var \Elasticsearch\Endpoints\Xpack\Security\User\Get $endpoint */
        $endpoint = $endpointBuilder('Xpack\Security\User\Get');
        $endpoint->setUsername($username)
            ->setParams($params);

        return $this->performRequest($endpoint);
    }
}