<?php

declare(strict_types = 1);

namespace Elasticsearch\Tests\ConnectionPool;

use Elasticsearch\ConnectionPool\Selectors\RoundRobinSelector;
use Elasticsearch\ConnectionPool\SniffingConnectionPool;
use Elasticsearch\Connections\Connection;
use Elasticsearch\Connections\ConnectionFactory;
use Mockery as m;
use Elasticsearch\Common\Exceptions\Curl\OperationTimeoutException;

/**
 * Class SniffingConnectionPoolTest
 *
 * @category   Tests
 * @package    Elasticsearch
 * @subpackage Tests/SniffingConnectionPoolTest
 * @author     Zachary Tong <zachary.tong@elasticsearch.com>
 * @license    http://www.apache.org/licenses/LICENSE-2.0 Apache2
 * @link       http://elasticsearch.org
 */
class SniffingConnectionPoolTest extends \PHPUnit_Framework_TestCase
{
    public function tearDown()
    {
        m::close();
    }

    public function testAddOneHostThenGetConnection()
    {
        $mockConnection = m::mock(Connection::class)
                          ->shouldReceive('ping')
                          ->andReturn(true)
                          ->getMock()
                          ->shouldReceive('isAlive')
                          ->andReturn(true)
                          ->getMock();

        $connections = array($mockConnection);

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')
                    ->andReturn($connections[0])
                    ->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class);

        $connectionPoolParams = array('randomizeHosts' => false);
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();

        $this->assertSame($mockConnection, $retConnection);
    }

    public function testAddOneHostAndTriggerSniff()
    {
        $clusterState = json_decode('{"ok":true,"cluster_name":"elasticsearch_zach","nodes":{"Bl2ihSr7TcuUHxhu1GA_YQ":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9300]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9200]"}}}', true);

        $mockConnection = m::mock(Connection::class)
                          ->shouldReceive('ping')->andReturn(true)->getMock()
                          ->shouldReceive('isAlive')->andReturn(true)->getMock()
                          ->shouldReceive('getTransportSchema')->once()->andReturn('http')->getMock()
                          ->shouldReceive('sniff')->once()->andReturn($clusterState)->getMock();

        $connections = array($mockConnection);
        $mockNewConnection = m::mock(Connection::class)
                             ->shouldReceive('isAlive')->andReturn(true)->getMock();

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')->twice()
                    ->andReturn($mockNewConnection)
                    ->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class)
                    ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9200))->andReturn($mockNewConnection)->getMock();

        $connectionPoolParams = array(
            'randomizeHosts' => false,
            'sniffingInterval'  => -1
        );
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();

        $this->assertSame($mockNewConnection, $retConnection);
    }

    public function testAddOneHostAndForceNext()
    {
        $clusterState = json_decode('{"ok":true,"cluster_name":"elasticsearch_zach","nodes":{"Bl2ihSr7TcuUHxhu1GA_YQ":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9300]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9200]"}}}', true);

        $mockConnection = m::mock(Connection::class)
                          ->shouldReceive('ping')->andReturn(true)->getMock()
                          ->shouldReceive('isAlive')->andReturn(true)->getMock()
                          ->shouldReceive('getTransportSchema')->once()->andReturn('http')->getMock()
                          ->shouldReceive('sniff')->once()->andReturn($clusterState)->getMock();

        $connections = array($mockConnection);
        $mockNewConnection = m::mock(Connection::class)
                             ->shouldReceive('isAlive')->andReturn(true)->getMock();

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')->once()->andReturn($mockConnection)->getMock()
                    ->shouldReceive('select')->once()->andReturn($mockNewConnection)->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class)
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9200))->andReturn($mockNewConnection)->getMock();

        $connectionPoolParams = array(
            'randomizeHosts' => false
        );
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection(true);

        $this->assertSame($mockNewConnection, $retConnection);
    }

    public function testAddTenNodesThenGetConnection()
    {
        $connections = array();

        foreach (range(1, 10) as $index) {
            $mockConnection = m::mock(Connection::class)
                              ->shouldReceive('ping')
                              ->andReturn(true)
                              ->getMock()
                              ->shouldReceive('isAlive')
                              ->andReturn(true)
                              ->getMock();

            $connections[] = $mockConnection;
        }

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')
                    ->andReturn($connections[0])
                    ->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class);

        $connectionPoolParams = array('randomizeHosts' => false);
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();

        $this->assertSame($connections[0], $retConnection);
    }

    public function testAddTenNodesTimeoutAllButLast()
    {
        $connections = array();

        foreach (range(1, 9) as $index) {
            $mockConnection = m::mock(Connection::class)
                              ->shouldReceive('ping')
                              ->andReturn(false)
                              ->getMock()
                              ->shouldReceive('isAlive')
                              ->andReturn(false)
                              ->getMock();

            $connections[] = $mockConnection;
        }

        $mockConnection = m::mock(Connection::class)
                          ->shouldReceive('ping')
                          ->andReturn(true)
                          ->getMock()
                          ->shouldReceive('isAlive')
                          ->andReturn(true)
                          ->getMock();

        $connections[] = $mockConnection;

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')
                    ->andReturnValues($connections)
                    ->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class);

        $connectionPoolParams = array('randomizeHosts' => false);
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();

        $this->assertSame($connections[9], $retConnection);
    }

    /**
     * @expectedException \Elasticsearch\Common\Exceptions\NoNodesAvailableException
     */
    public function testAddTenNodesAllTimeout()
    {
        $connections = array();

        foreach (range(1, 10) as $index) {
            $mockConnection = m::mock(Connection::class)
                              ->shouldReceive('ping')
                              ->andReturn(false)
                              ->getMock()
                              ->shouldReceive('isAlive')
                              ->andReturn(false)
                              ->getMock();

            $connections[] = $mockConnection;
        }

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')
                    ->andReturnValues($connections)
                    ->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class);

        $connectionPoolParams = array('randomizeHosts' => false);
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();
    }

    public function testAddOneHostSniffTwo()
    {
        $clusterState = json_decode('{"ok":true,"cluster_name":"elasticsearch_zach","nodes":{"node1":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9300]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9200]"}, "node2":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9301]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9201]"}}}', true);

        $mockConnection = m::mock(Connection::class)
                          ->shouldReceive('ping')->andReturn(true)->getMock()
                          ->shouldReceive('isAlive')->andReturn(true)->getMock()
                          ->shouldReceive('getTransportSchema')->twice()->andReturn('http')->getMock()
                          ->shouldReceive('sniff')->twice()->andReturn($clusterState)->getMock();

        $connections = array($mockConnection);

        $newConnections = array();
        $newConnections[] = m::mock(Connection::class)
                             ->shouldReceive('isAlive')->andReturn(true)->getMock();

        $newConnections[] = m::mock(Connection::class)
                             ->shouldReceive('isAlive')->andReturn(true)->getMock();

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')
                    ->andReturnValues(array(        //selects provided node first, then the new cluster list
                            $mockConnection,
                            $newConnections[0],
                            $newConnections[1]
                    ))
                    ->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class)
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9200))->andReturn($newConnections[0])->getMock()
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9201))->andReturn($newConnections[1])->getMock();

        $connectionPoolParams = array(
            'randomizeHosts' => false,
            'sniffingInterval'  => -1
        );
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();
        $this->assertSame($newConnections[0], $retConnection);

        $retConnection = $connectionPool->nextConnection();
        $this->assertSame($newConnections[1], $retConnection);
    }

    /**
     * @expectedException \Elasticsearch\Common\Exceptions\NoNodesAvailableException
     */
    public function testAddSeed_SniffTwo_TimeoutTwo()
    {
        $clusterState = json_decode('{"ok":true,"cluster_name":"elasticsearch_zach","nodes":{"node1":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9300]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9200]"}, "node2":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9301]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9201]"}}}', true);

        $mockConnection = m::mock(Connection::class)
                          ->shouldReceive('ping')->andReturn(true)->getMock()
                          ->shouldReceive('isAlive')->andReturn(true)->getMock()
                          ->shouldReceive('getTransportSchema')->once()->andReturn('http')->getMock()
                          ->shouldReceive('sniff')->once()->andReturn($clusterState)->getMock();

        $connections = array($mockConnection);

        $newConnections = array();
        $newConnections[] = m::mock(Connection::class)
                            ->shouldReceive('isAlive')->andReturn(false)->getMock()
                            ->shouldReceive('ping')->andReturn(false)->getMock();

        $newConnections[] = m::mock(Connection::class)
                            ->shouldReceive('isAlive')->andReturn(false)->getMock()
                            ->shouldReceive('ping')->andReturn(false)->getMock();

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')
                    ->andReturnValues(array(        //selects provided node first, then the new cluster list
                    $mockConnection,
                    $newConnections[0],
                    $newConnections[1]
                ))
                    ->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class)
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9200))->andReturn($newConnections[0])->getMock()
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9201))->andReturn($newConnections[1])->getMock();

        $connectionPoolParams = array(
            'randomizeHosts' => false,
            'sniffingInterval'  => -1
        );
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();
        $this->assertSame($mockConnection, $retConnection);
    }

    public function testTen_TimeoutNine_SniffTenth_AddTwoAlive()
    {
        $clusterState = json_decode('{"ok":true,"cluster_name":"elasticsearch_zach","nodes":{"node1":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9300]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9200]"}, "node2":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9301]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9201]"}}}', true);

        $connections = array();

        foreach (range(1, 10) as $index) {
            $mockConnection = m::mock(Connection::class)
                              ->shouldReceive('ping')->andReturn(false)->getMock()
                              ->shouldReceive('isAlive')->andReturn(true)->getMock()
                              ->shouldReceive('sniff')->andThrow(OperationTimeoutException::class)->getMock();

            $connections[] = $mockConnection;
        }

        $mockConnection = m::mock(Connection::class)
                          ->shouldReceive('ping')->andReturn(true)->getMock()
                          ->shouldReceive('isAlive')->andReturn(true)->getMock()
                          ->shouldReceive('sniff')->andReturn($clusterState)->getMock()
                          ->shouldReceive('getTransportSchema')->twice()->andReturn('http')->getMock();

        $connections[] = $mockConnection;

        $newConnections = $connections;
        $newConnections[] = m::mock(Connection::class)
                            ->shouldReceive('isAlive')->andReturn(true)->getMock()
                            ->shouldReceive('ping')->andReturn(true)->getMock();

        $newConnections[] = m::mock(Connection::class)
                            ->shouldReceive('isAlive')->andReturn(true)->getMock()
                            ->shouldReceive('ping')->andReturn(true)->getMock();

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')
                    ->andReturnValues($newConnections)
                    ->getMock();

        $connectionFactory = m::mock(ConnectionFactory::class)
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9200))->andReturn($newConnections[10])->getMock()
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9201))->andReturn($newConnections[11])->getMock();

        $connectionPoolParams = array(
            'randomizeHosts' => false,
            'sniffingInterval'  => -1
        );
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();
        $this->assertSame($newConnections[11], $retConnection);

        $retConnection = $connectionPool->nextConnection();
        $this->assertSame($newConnections[12], $retConnection);
    }

    /**
     * @expectedException \Elasticsearch\Common\Exceptions\NoNodesAvailableException
     */
    public function testTen_TimeoutNine_SniffTenth_AddTwoDead_TimeoutEveryone()
    {
        $clusterState = json_decode('{"ok":true,"cluster_name":"elasticsearch_zach","nodes":{"node1":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9300]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9200]"}, "node2":{"name":"Vesta","transport_address":"inet[/192.168.1.119:9301]","hostname":"zach-ThinkPad-W530","version":"0.90.5","http_address":"inet[/192.168.1.119:9201]"}}}', true);

        $connections = array();

        foreach (range(1, 10) as $index) {
            $mockConnection = m::mock(Connection::class)
                              ->shouldReceive('ping')->andReturn(false)->getMock()
                              ->shouldReceive('isAlive')->andReturn(true)->getMock()
                              ->shouldReceive('sniff')->andThrow(OperationTimeoutException::class)->getMock();

            $connections[] = $mockConnection;
        }

        $mockConnection = m::mock(Connection::class)
                          ->shouldReceive('ping')->andReturn(true)->getMock()
                          ->shouldReceive('isAlive')->andReturn(true)->getMock()
                          ->shouldReceive('sniff')->andReturn($clusterState)->getMock()
                          ->shouldReceive('getTransportSchema')->once()->andReturn('http')->getMock()
                          ->shouldReceive('sniff')->andThrow(OperationTimeoutException::class)->getMock();

        $connections[] = $mockConnection;

        $newConnections = $connections;
        $newConnections[] = m::mock(Connection::class)
                            ->shouldReceive('isAlive')->andReturn(false)->getMock()
                            ->shouldReceive('ping')->andReturn(false)->getMock()
                            ->shouldReceive('sniff')->andThrow(OperationTimeoutException::class)->getMock();

        $newConnections[] = m::mock(Connection::class)
                            ->shouldReceive('isAlive')->andReturn(false)->getMock()
                            ->shouldReceive('ping')->andReturn(false)->getMock()
                            ->shouldReceive('sniff')->andThrow(OperationTimeoutException::class)->getMock();

        $selector = m::mock(RoundRobinSelector::class)
                    ->shouldReceive('select')
                    ->andReturnValues($newConnections)
                    ->getMock();

        $RRConnections = $newConnections;
        //array_push($connections);
        $connectionFactory = m::mock(ConnectionFactory::class)
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9200))->andReturn($newConnections[10])->getMock()
                             ->shouldReceive('create')->with(array('host' => '192.168.1.119', 'port' => 9201))->andReturn($newConnections[11])->getMock();

        $connectionPoolParams = array(
            'randomizeHosts' => false,
            'sniffingInterval'  => -1
        );
        $connectionPool = new SniffingConnectionPool($connections, $selector, $connectionFactory, $connectionPoolParams);

        $retConnection = $connectionPool->nextConnection();
        $this->assertSame($newConnections[11], $retConnection);

        $retConnection = $connectionPool->nextConnection();
        $this->assertSame($newConnections[12], $retConnection);
    }
}
