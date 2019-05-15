package com.raja.framework.cache.redis.config;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import redis.clients.jedis.Jedis;

/**
 * The Class RedisAvailabilityCondition.
 */
public class RedisAvailabilityCondition implements Condition {

	private final Logger log = LoggerFactory.getLogger(RedisAvailabilityCondition.class);

	/**
	 * Instantiates a new redis availability condition.
	 */
	public RedisAvailabilityCondition() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.context.annotation.Condition#matches(org.springframework.
	 * context.annotation.ConditionContext,
	 * org.springframework.core.type.AnnotatedTypeMetadata)
	 */
	@Override
	public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
		try {

			List<String> activeProfiles = Arrays.asList(context.getEnvironment().getActiveProfiles());
			log.info("Active Profile: " + activeProfiles.toString());
			String host = context.getEnvironment().getProperty("spring.redis.host");
			String port = context.getEnvironment().getProperty("spring.redis.port");
			String maxRedirects = context.getEnvironment().getProperty("spring.redis.cluster.max-redirects");
			List<String> listOfRedisClusterNodes = context.getEnvironment().getProperty("spring.redis.cluster.nodes", List.class);

			log.info("************ Redis Cluster Information **********");
			log.info("Host Name:" + host);
			log.info("Port:" + port);
			log.info("Redis Cluster Nodes:" + listOfRedisClusterNodes);

			if (activeProfiles.contains("local") && (host != null && port != null)) {

				log.info("Connecting Using Single Redis insatnce....");
				Jedis jedis = new Jedis(host, Integer.valueOf(port));
				JedisConnection connection = new JedisConnection(jedis);
				String response = connection.ping();
				log.info("Response from Single redis Instance:" + response);
				if (null != response) {
					return true;
				}
			} else if (null != listOfRedisClusterNodes && !listOfRedisClusterNodes.isEmpty()) {

				log.info("Connecting to Redis Cluster........");
				RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration(listOfRedisClusterNodes);
				redisClusterConfiguration.setMaxRedirects(Integer.valueOf(maxRedirects));
				JedisConnectionFactory jedisConectionfactory = new JedisConnectionFactory(redisClusterConfiguration);
				jedisConectionfactory.afterPropertiesSet();

				RedisClusterConnection clusterConnection = jedisConectionfactory.getClusterConnection();
				String pong = clusterConnection.ping();
				log.info("Redis Cluster Connection response:" + pong);
				if (null != pong) {
					return true;
				}
			}
		} catch (Exception e) {
			log.error("Redis Connectivity Error");
			log.error(e.getMessage());
			return false;
		}
		return false;
	}

}
