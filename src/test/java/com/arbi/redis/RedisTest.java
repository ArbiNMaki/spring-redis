package com.arbi.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.support.collections.*;

import java.time.Duration;
import java.util.*;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class RedisTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private CacheManager cacheManager;

    @Test
    void testRedisTemplate() {
        assertNotNull(redisTemplate);
    }

    @Test
    void string() throws InterruptedException {
        ValueOperations<String, String> operations = redisTemplate.opsForValue();

        operations.set("name", "Arbi", Duration.ofSeconds(2));
        assertEquals("Arbi", operations.get("name"));

        Thread.sleep(Duration.ofSeconds(3));
        assertNull(operations.get("name"));
    }

    @Test
    void list() {
        ListOperations<String, String> operations = redisTemplate.opsForList();

        operations.rightPush("names", "Arbi");
        operations.rightPush("names", "Dwi");
        operations.rightPush("names", "Wijaya");

        assertEquals("Arbi", operations.leftPop("names"));
        assertEquals("Dwi", operations.leftPop("names"));
        assertEquals("Wijaya", operations.leftPop("names"));
    }

    @Test
    void set() {
        SetOperations<String, String> operations = redisTemplate.opsForSet();

        operations.add("students", "Arbi");
        operations.add("students", "Arbi");
        operations.add("students", "Dwi");
        operations.add("students", "Dwi");
        operations.add("students", "Wijaya");
        operations.add("students", "Wijaya");

        Set<String> students = operations.members("students");
        assertEquals(3, students.size());
        assertThat(students, hasItems("Arbi", "Dwi", "Wijaya"));
    }

    @Test
    void zSet() {
        ZSetOperations<String, String> operations = redisTemplate.opsForZSet();

        operations.add("score", "Arbi", 100);
        operations.add("score", "Maki", 90);
        operations.add("score", "Katsuki", 95);

        assertEquals("Arbi", operations.popMax("score").getValue());
        assertEquals("Katsuki", operations.popMax("score").getValue());
        assertEquals("Maki", operations.popMax("score").getValue());
    }

    @Test
    void hash() {
        HashOperations<String, Object, Object> operations = redisTemplate.opsForHash();

//        operations.put("user:1", "id", "1");
//        operations.put("user:1", "name", "Arbi");
//        operations.put("user:1", "email", "arbi@example.com");

        Map<Object, Object> map = new HashMap<>();
        map.put("id", "1");
        map.put("name", "Arbi");
        map.put("email", "arbi@example.com");
        operations.putAll("user:1", map);

        assertEquals("1", operations.get("user:1", "id"));
        assertEquals("Arbi", operations.get("user:1", "name"));
        assertEquals("arbi@example.com", operations.get("user:1", "email"));

        redisTemplate.delete("user:1");
    }

    @Test
    void geo() {
        GeoOperations<String, String> operations = redisTemplate.opsForGeo();

        operations.add("sellers", new Point(106.822695, -6.177456), "Toko A");
        operations.add("sellers", new Point(106.821016, -6.174598), "Toko B");

        Distance distance = operations.distance("sellers", "Toko A", "Toko B", Metrics.KILOMETERS);
        assertEquals(0.3682, distance.getValue());

        GeoResults<RedisGeoCommands.GeoLocation<String>> sellers =
                operations.search("sellers", new Circle(
                    new Point(106.821922, -6.175491),
                    new Distance(5, Metrics.KILOMETERS)
                ));

        assertEquals(2, sellers.getContent().size());
        assertEquals("Toko A",
                sellers.getContent()
                        .get(0)
                        .getContent()
                        .getName());
        assertEquals("Toko B",
                sellers.getContent()
                        .get(1)
                        .getContent()
                        .getName());
    }

    @Test
    void hyperLogLog() {
        HyperLogLogOperations<String, String> operations = redisTemplate.opsForHyperLogLog();

        operations.add("traffics", "arbi", "dwi", "wijaya");
        operations.add("traffics", "arbi", "katsuki", "maki");
        operations.add("traffics", "katsuki", "maki", "kalista");

        assertEquals(6L, operations.size("traffics"));
    }

    @Test
    void transaction() {
        redisTemplate.execute(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();

                operations.opsForValue().set("test1", "Arbi", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Kalista", Duration.ofSeconds(2));

                operations.exec();
                return null;
            }
        });

        assertEquals("Arbi", redisTemplate.opsForValue().get("test1"));
        assertEquals("Kalista", redisTemplate.opsForValue().get("test2"));
    }

    @Test
    void pipeline() {
        List<Object> list = redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("test1", "Arbi", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Katsuki", Duration.ofSeconds(2));
                operations.opsForValue().set("test3", "Maki", Duration.ofSeconds(2));
                operations.opsForValue().set("test4", "Kalista", Duration.ofSeconds(2));

                return null;
            }
        });

        assertThat(list, hasSize(4));
        assertThat(list, hasItem(true));
        assertThat(list, not(hasItem(false)));
    }

    @Test
    void publishStream() {
        var operations = redisTemplate.opsForStream();
        var record = MapRecord.create("stream-1", Map.of(
                "name", "Arbi Dwi Wijaya",
                "address", "Indonesia"
        ));

        for (int i = 0; i < 10; i++) {
            operations.add(record);
        }
    }

    @Test
    void subscribeStream() {
        var operations = redisTemplate.opsForStream();

        try {
            operations.createGroup("stream-1", "sample-group");
        } catch (RedisSystemException exception) {
            // group already exists
        }

        List<MapRecord<String, Object, Object>> records = operations.read(Consumer.from("sample-group", "sample-1"),
                StreamOffset.create("stream-1", ReadOffset.lastConsumed()));

        for (MapRecord<String, Object, Object> record : records) {
            System.out.println(record);
        }
    }

    @Test
    void pubSub() {
        redisTemplate.getConnectionFactory().getConnection().subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                String event = new String(message.getBody());
                System.out.println("Receive message : " + event);
            }
        }, "my-channel".getBytes());

        for (int i = 0; i < 10; i++) {
            redisTemplate.convertAndSend("my-channel", "Hello World : " + i);
        }
    }

    @Test
    void redisList() {
        List<String> list = RedisList.create("names", redisTemplate);
        list.add("Arbi");
        list.add("Dwi");
        list.add("Wijaya");
        assertThat(list, hasItems("Arbi", "Dwi", "Wijaya"));

        List<String> result = redisTemplate
                .opsForList()
                .range("names", 0, -1);

        assertThat(result, hasItems("Arbi", "Dwi", "Wijaya"));
    }

    @Test
    void redisSet() {
        Set<String> set = RedisSet.create("traffic", redisTemplate);
        set.addAll(Set.of("arbi", "dwi", "wijaya"));
        set.addAll(Set.of("arbi", "katsuki", "maki"));
        assertThat(set, hasItems("arbi", "dwi", "wijaya", "katsuki", "maki"));

        Set<String> members = redisTemplate.opsForSet().members("traffic");
        assertThat(members, hasItems("arbi", "dwi", "wijaya", "katsuki", "maki"));
    }

    @Test
    void redisZSet() {
        RedisZSet<String> zset = RedisZSet.create("winner", redisTemplate);
        zset.add("Arbi", 100);
        zset.add("Katsuki", 95);
        zset.add("Maki", 90);
        assertThat(zset, hasItems("Arbi", "Katsuki", "Maki"));

        Set<String> members = redisTemplate.opsForZSet().range("winner", 0, -1);
        assertThat(members, hasItems("Arbi", "Katsuki", "Maki"));

        assertEquals("Arbi", zset.popLast());
        assertEquals("Katsuki", zset.popLast());
        assertEquals("Maki", zset.popLast());
    }

    @Test
    void redisMap() {
        Map<String, String> map = new DefaultRedisMap<>("user:1", redisTemplate);
        map.put("name", "Arbi");
        map.put("address", "Indonesia");
        assertThat(map, hasEntry("name", "Arbi"));
        assertThat(map, hasEntry("address", "Indonesia"));

        Map<Object, Object> user = redisTemplate.opsForHash().entries("user:1");
        assertThat(user, hasEntry("name", "Arbi"));
        assertThat(user, hasEntry("address", "Indonesia"));
    }

    @Test
    void repository() {
        Product product = Product.builder()
                .id("1")
                .name("Mie Ayam Jakarta")
                .price(20_000L)
                .build();
        productRepository.save(product);

        Map<Object, Object> map = redisTemplate
                .opsForHash()
                .entries("products:1");
        assertEquals(product.getId(), map.get("id"));
        assertEquals(product.getName(), map.get("name"));
        assertEquals(product.getPrice().toString(), map.get("price"));

        Product product2 = productRepository.findById("1").get();
        assertEquals(product, product2);
    }

    @Test
    void ttl() throws InterruptedException {
        Product product = Product.builder()
                .id("1")
                .name("Mie Ayam Jakarta")
                .price(20_000L)
                .ttl(3L)
                .build();
        productRepository.save(product);

        assertTrue(productRepository.findById("1").isPresent());
        Thread.sleep(Duration.ofSeconds(5));

        assertFalse(productRepository.findById("1").isPresent());
    }

    @Test
    void cache() {
        Cache sample = cacheManager.getCache("scores");
        sample.put("Arbi", 100);
        sample.put("Katsuki", 95);
        sample.put("Maki", 90);

        assertEquals(100, sample.get("Arbi", Integer.class));
        assertEquals(95, sample.get("Katsuki", Integer.class));
        assertEquals(90, sample.get("Maki", Integer.class));

        sample.evict("Arbi");
        sample.evict("Katsuki");
        sample.evict("Maki");
        assertNull(sample.get("Arbi", Integer.class));
        assertNull(sample.get("Katsuki", Integer.class));
        assertNull(sample.get("Maki", Integer.class));
    }
}
