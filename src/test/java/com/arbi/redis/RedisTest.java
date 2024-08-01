package com.arbi.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
}
