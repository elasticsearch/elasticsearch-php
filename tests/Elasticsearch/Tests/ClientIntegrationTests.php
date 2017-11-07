<?php

declare(strict_types=1);

namespace Elasticsearch\Tests;

use Elasticsearch;
use stdClass;

/**
 * Class ClientTest
 *
 * @category   Tests
 * @package    Elasticsearch
 * @subpackage Tests
 * @author     Zachary Tong <zachary.tong@elasticsearch.com>
 * @license    http://www.apache.org/licenses/LICENSE-2.0 Apache2
 * @link       http://elasticsearch.org
 */
class ClientIntegrationTests extends \PHPUnit\Framework\TestCase
{

    /**
     * @var $client Elasticsearch\Client
     */
    protected static $client;

    /**
     * initialize elasticsearch client
     */
    public static function setUpBeforeClass()
    {
        self::$client = Elasticsearch\ClientBuilder::create()->setHosts([$_SERVER['ES_TEST_HOST']])->build();
    }

    public function tearDown()
    {
        // remove all previously indices created by test or by the before setup
        $param = [
            'index' => '_all'
        ];
        self::$client->indices()->delete($param);
    }

    public function testCustomQueryParams()
    {
        self::$client = Elasticsearch\ClientBuilder::create()->setHosts([$_SERVER['ES_TEST_HOST']])->build();

        $getParams = [
            'index' => 'test',
            'type' => 'test',
            'id' => 1,
            'parent' => 'abc',
            'custom' => ['customToken' => 'abc', 'otherToken' => 123],
            'client' => ['ignore' => 400]
        ];
        $exists = self::$client->exists($getParams);

        $this->assertFalse($exists);
    }

    /**
     * @dataProvider aliasDataProvider
     */
    public function testExistAlias($alias)
    {
        $this->createIndexWithAlias($alias);

        $params = array(
            'name' => $alias
        );

        $this->assertTrue(self::$client->indices()->existsAlias($params));
    }

    /**
     * @dataProvider aliasDataProvider
     */
    public function testFindIndexByAlias($alias)
    {
        $this->createIndexWithAlias($alias);

        $params = array(
            'name' => $alias
        );
        $this->assertEquals($alias . '_v1', array_keys(self::$client->indices()->getAlias($params))[0]);
    }

    /**
     * @dataProvider aliasDataProvider
     */
    public function testPutAlias($alias)
    {

        $this->createIndexWithoutAlias($alias);

        $params = array(
            'index' => $alias . '_v1',
            'name' => $alias
        );

        self::$client->indices()->putAlias($params);

        $params = array(
            'name' => $alias
        );
        $this->assertEquals($alias . '_v1', array_keys(self::$client->indices()->getAlias($params))[0]);
    }

    public function aliasDataProvider()
    {
        return [
            'latin-char' => ['myindextest'],
            'utf-8-char' => ['⿇⽸⾽']
        ];
    }

    private function createIndexWithAlias($alias)
    {
        $params = array(
            'index' => $alias . '_v1',
            'body' => array(
                'aliases' => array(
                    $alias => new stdClass()
                )),
        );

        self::$client->indices()->create($params);
    }

    private function createIndexWithoutAlias($alias)
    {
        $params = array('index' => $alias . '_v1');

        self::$client->indices()->create($params);
    }

}
