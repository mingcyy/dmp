package com.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisPools {

//    默认存储的是0号库
//    private val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "192.168.52.100", 6379)
//    存储到8号库
    private val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "192.168.52.100", 6379,3000,"123456",8)

    def getJedis() = jedisPool.getResource

}
