package org.example.cross;

import redis.clients.jedis.Jedis;

public class RedisConnection {

    private final String host;
    private final int port;
    private final int timeout;

    public RedisConnection(String host, int port, int timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
    }

    public Jedis getConnection() {
        return new Jedis(host, port, timeout);
    }
}
