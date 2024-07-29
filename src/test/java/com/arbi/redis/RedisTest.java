package com.arbi.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.*;

import java.time.Duration;
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
}
