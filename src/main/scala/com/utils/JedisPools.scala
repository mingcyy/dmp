package com.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisPools {

    private val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "192.168.52.100", 6379)


    def getJedis() = jedisPool.getResource

}
