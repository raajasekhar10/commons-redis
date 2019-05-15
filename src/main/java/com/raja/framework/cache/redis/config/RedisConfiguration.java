package com.raja.framework.cache.redis.config;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import org.springframework.util.Assert;
import redis.clients.jedis.JedisPoolConfig;

/**
 * <p>
 * Configuration defined for REDIS Connectivity, Using the Jedis client, a REDIS
 * connectionFactory can be defined.
 * <p>
 * To enable support for caching in our application, we'll create a new
 * CacheManager bean. There are many different implementations of the
 * CacheManager interface, but we'll be using RedisCacheManager to allow
 * integration with Redis.
 * </p>
 */
@Configuration
@EnableCaching
@ConditionalOnProperty(name = "service.redis.cache.enabled")
@EnableConfigurationProperties(RedisConfigurationProperties.class)
public class RedisConfiguration extends CachingConfigurerSupport {

    /**
     * The Constant LOCAL_PROFILE.
     */
    private static final String LOCAL_PROFILE = "local";

    /**
     * The log.
     */
    private final Logger log = LoggerFactory.getLogger(RedisConfiguration.class);

    /**
     * The redis host name.
     */
    @Value("${spring.redis.host:localhost}")
    private Optional<String> redisHostName;

    @Value("${spring.redis.sentinel.master:localhost}")
    private String redisSentinelMaster;


    @Value("${spring.redis.sentinel.nodes:localhost}")
    private String redisSentinelNodes;


    @Value("${spring.redis.password:XXXXX}")
    private String redisPassword;

    /**
     * The redis port.
     */
    @Value("${spring.redis.port:6379}")
    private Optional<Integer> redisPort;

    /**
     * The list of redis cluster nodes.
     */
    // cluster nodes
    @Value("#{'${spring.redis.cluster.nodes:localhost:#{null}}'.split(',')}")
    private Optional<List<String>> listOfRedisClusterNodes;


    /**
     * The max redirects.
     */
    @Value("${spring.redis.cluster.max-redirects:3}")
    private Optional<Integer> maxRedirects;


    /**
     * The environment.
     */
    @Autowired
    Environment environment;

    /**
     * The database index.
     */
    @Value("${service.redis.cache.databaseIndex:1}")
    private int databaseIndex;

    @Value("${service.redis.timeout:500}")
    private int timeout;


    /**
     * The jedis pool size.
     */
    @Value("${service.redis.cache.jedisPoolSize:10}")
    private int jedisPoolSize;

    /**
     * The defaultEntryExpiry
     */
    @Value("${service.redis.cache.defaultEntryExpiry:3600}")
    private Optional<Integer> defaultEntryExpiry;

    /**
     * Gets the database index.
     *
     * @return the database index
     */
    public int getDatabaseIndex() {
        return databaseIndex;
    }

    /**
     * Sets the database index.
     *
     * @param databaseIndex the new database index
     */
    public void setDatabaseIndex(int databaseIndex) {
        this.databaseIndex = databaseIndex;
    }

    /**
     * Gets the jedis pool size.
     *
     * @return the jedis pool size
     */
    public int getJedisPoolSize() {
        return jedisPoolSize;
    }

    /**
     * Sets the jedis pool size.
     *
     * @param jedisPoolSize the new jedis pool size
     */
    public void setJedisPoolSize(int jedisPoolSize) {
        this.jedisPoolSize = jedisPoolSize;
    }


    private RedisNode readHostAndPortFromString(String hostAndPort) {
        log.info("Redis host and port " + hostAndPort);
        String[] args = org.springframework.util.StringUtils.split(hostAndPort, ":");
        Assert.notNull(args, "HostAndPort need to be separated by  ':'.");
        Assert.isTrue(args.length == 2, "Host and Port String needs to specified as host:port");
        return new RedisNode(args[0], Integer.valueOf(args[1]));
    }


    /**
     * <p>
     * Redis configuration with Jedis starts from defining JedisConnectionFactory.
     * By default, Jedis uses connection pool
     * (http://en.wikipedia.org/wiki/Connection_pool) in order not to create
     * connections to the Redis server every time but rather borrow them from the
     * pool of available connections. Overall it is considered as a good practice
     * because the process of creating network connections is a relatively expensive
     * operation.
     * <p>
     * Configuring the connection factory to Redis instance running on localhost
     * with a pool of maximum 10 connections.
     * </p>
     *
     * @return the jedis connection factory
     */
    @Bean
    public JedisConnectionFactory createConnectionFactory() {

        log.info("([Inside RedisConfiguration.createConnectionFactory()]");
        JedisConnectionFactory jedisConnectionFactory;

        // if it is a non cluster environment
        List<String> activeProfiles = Arrays.asList(environment.getActiveProfiles());

        log.info("Active Profile: " + activeProfiles.toString());
        log.info("Redis Host Name:" + redisHostName.get());
        log.info("Redis Port:" + redisPort.get());
        log.info("Redis Cluster Nodes:" + listOfRedisClusterNodes.get());
        log.info("Redis Sentinel Master :" + redisSentinelMaster);
        log.info("Redis Sentinel Nodes :" + redisSentinelNodes);

        
        if (StringUtils.isNotBlank(redisSentinelMaster) && StringUtils.isNotBlank(redisSentinelNodes) &&
        		!StringUtils.equalsIgnoreCase(redisSentinelMaster,"localhost") && !StringUtils.equalsIgnoreCase(redisSentinelNodes,"localhost") ) {
            log.info("Using Redis Sentinel ....");
            RedisSentinelConfiguration sentinelConfig = new RedisSentinelConfiguration()
                    .master(redisSentinelMaster);
            org.springframework.util.StringUtils.commaDelimitedListToSet(redisSentinelNodes).forEach(e -> sentinelConfig.addSentinel(readHostAndPortFromString(e)));
            jedisConnectionFactory = new JedisConnectionFactory(sentinelConfig);
            jedisConnectionFactory.setPassword(redisPassword);
            jedisConnectionFactory.setTimeout(timeout);
            return jedisConnectionFactory;
        }


        if (redisHostName.isPresent() && redisPort.isPresent()) {
            log.info("Using Redis Host ....");
            log.info("Redis Host Name for Local: " + redisHostName.get());
            log.info("Redis Port for Local: " + redisPort.get());
            log.info("Setting databaseIndex: " + databaseIndex);

            jedisConnectionFactory = new JedisConnectionFactory(poolConfig());
            jedisConnectionFactory.setHostName(redisHostName.get());
            jedisConnectionFactory.setPort(redisPort.get());
            jedisConnectionFactory.setDatabase(databaseIndex);
            jedisConnectionFactory.setUsePool(Boolean.TRUE);
            jedisConnectionFactory.setTimeout(timeout);
            log.info("Redis Connection Factory is : {}", jedisConnectionFactory);
            return jedisConnectionFactory;

        }

        log.info("Using Redis Cluster ....");
        // For clustered environments.
        log.info("list Of Redis Cluster Nodes for Cluster: " + listOfRedisClusterNodes.get());
        log.info("Setting databaseIndex: " + databaseIndex);
        RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration(listOfRedisClusterNodes.get());
        log.info("Max Redirects for Cluster: " + maxRedirects.get());
        redisClusterConfiguration.setMaxRedirects(maxRedirects.get());
        jedisConnectionFactory = new JedisConnectionFactory(redisClusterConfiguration);
        jedisConnectionFactory.setDatabase(databaseIndex);
        jedisConnectionFactory.setUsePool(Boolean.TRUE);
        jedisConnectionFactory.afterPropertiesSet();
        jedisConnectionFactory.setTimeout(timeout);

//		if (!activeProfiles.contains(LOCAL_PROFILE)) {
//			
//			log.info("Non Local Redis Cluster Factory: ");
//			RedisClusterConnection redisClusterConnection = jedisConnectionFactory.getClusterConnection();
//			if (null != redisClusterConnection) {
//				log.info("Redis Cluster Info:");
//				ClusterInfo clusterInfo = redisClusterConnection.clusterGetClusterInfo();
//
//				if (null != clusterInfo) {
//					log.info("Cluster Size:" + clusterInfo.getClusterSize());
//					log.info("Known Nodes: " + clusterInfo.getKnownNodes());
//				}
//			}
//		}
        log.info("Redis Connection Factory is : {}", jedisConnectionFactory);
        return jedisConnectionFactory;
    }

    /**
     * <p>
     * Define connection pool as a separate Spring configuration bean so it could be
     * imported by different application configurations independently.
     * <p>
     * The test on borrow setting actually ensures that connection borrowed from the
     * pool is still valid and can be used (otherwise the connection will be
     * recreated).
     * </p>
     *
     * @return the jedis pool config
     */
    @Bean
    public JedisPoolConfig poolConfig() {

        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setMaxTotal(jedisPoolSize);
        return jedisPoolConfig;
    }

    /**
     * <p>
     * Using the jedisConnectionFactory, a RedisTemplate is defined, which will then
     * be used for querying data with a custom repository.
     * </p>
     *
     * @return the redis template
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate() {

        final RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
        template.setConnectionFactory(createConnectionFactory());
        RedisSerializer<Object> serializer = new JdkSerializationRedisSerializer(this.getClass().getClassLoader());
        template.setValueSerializer(serializer);
        template.setDefaultSerializer(serializer);
        template.setHashValueSerializer(serializer);
        template.setHashKeySerializer(serializer);
        template.setKeySerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    /* (non-Javadoc)
     * @see org.springframework.cache.annotation.CachingConfigurerSupport#cacheManager()
     */
    @Bean
    @Override
    public RedisCacheManager cacheManager() {

        RedisCacheManager cacheManager = new RedisCacheManager(redisTemplate());
        // Number of seconds before expiration. Defaults to unlimited (0)
        cacheManager.setDefaultExpiration(defaultEntryExpiry.get());
        cacheManager.setTransactionAware(Boolean.TRUE);
        // If we want to use already stored cache we need to turn flag under cacheManger
        // to true for the value setLoadRemoteCachesOnStartup.
        cacheManager.setLoadRemoteCachesOnStartup(Boolean.TRUE);
        cacheManager.setUsePrefix(Boolean.TRUE);
        return cacheManager;
    }
}
