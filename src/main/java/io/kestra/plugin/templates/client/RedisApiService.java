package io.kestra.plugin.templates.client;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;

@Singleton
public class RedisApiService {

    private Log log = LogFactory.getLog(RedisApiService.class);

    @Value("${redis.protocol}")
    protected String protocol;
    @Value("${redis.password}")
    protected String password;
    @Value("${redis.host}")
    protected String host;
    @Value("${redis.port}")
    protected String port;
    @Value("${redis.db}")
    protected int db;
    @Value("${redis.serdeType}")
    protected String serdeType = "STRING";


    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;

    public RedisApiService() {
        this.redisClient = RedisClient.create(protocol+"://"+password+"@"+host+":"+port+"/"+db);
    }
    public RedisApiService(String protocol, String password, String host, String port, int db, String serdeType) {
        this.protocol = protocol;
        this.password = password;
        this.host = host;
        this.port = port;
        this.db = db;
        this.serdeType = serdeType;

        this.redisClient = RedisClient.create(protocol+"://"+password+"@"+host+":"+port+"/"+db);
    }

    /**
     * Init connection
     * @return StatefulRedisConnection
     */
    public StatefulRedisConnection<String, String> connect() {
        try {
            return redisClient.connect();
        } catch (RedisCommandExecutionException e) {
            log.fatal("Failed to connect to REDIS");
            throw e;
        }
    }

    /**
     * Drop connection
     */
    public void disconnect() {
        connection.close();
        redisClient.shutdown();
    }

    /**
     * Return a stored value by key
     * @param key
     * @return
     */
    public String getString(String key) {
        RedisCommands<String, String> syncCommands = connection.sync();
        return syncCommands.get(key);
    }

    /**
     * Return a stored value by key as a JsonObject
     * @param key
     * @return
     */
    public JsonObject get(String key) {
        RedisCommands<String, String> syncCommands = connection.sync();
        String keyString = syncCommands.get(key);
        return JsonParser.parseString(keyString).getAsJsonObject();
    }

    /**
     * Store a value by key
     * @param key
     * @return
     */
    public String set(String key, String value) {
        if(connection == null) {
            this.connection = redisClient.connect();
        }

        RedisCommands<String, String> syncCommands = connection.sync();
        return syncCommands.set(key, value);
    }

    public void setSerdeType(String serdeType) {
        this.serdeType = serdeType;
    }

    /**
     * DELETES a key
     * @param key
     * @return
     */
    public HashMap<String, Object> delete(List<String> keys) {
        HashMap results = new HashMap();
        int removed = 0;
        if (connection == null) {
            this.connection = redisClient.connect();
        }

        RedisCommands<String, String> syncCommands = connection.sync();
        for (String key : keys) {
            long res = syncCommands.del(key);
            if(res != 0) {
                removed++;
            }
        }

        results.put("removed", removed);
        results.put("failedOnMissing", keys.size() - removed != 0);
        return results;
    }
}
