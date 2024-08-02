package com.arbi.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.*;

import java.time.Duration;
import java.util.*;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class RedisTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

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
    void name() {
    }
}
