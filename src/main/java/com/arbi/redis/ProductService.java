package com.arbi.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ProductService {

    @Cacheable(value = "products", key = "#id")
    public Product getProduct(String id) {
        log.info("get Product {}", id);
        return Product.builder()
                .id(id)
                .name("Sample")
                .build();
    }

    @Cacheable(value = "products", key = "#product.id")
    public Product save(Product product) {
        log.info("Sve Product {}", product);
        return product;
    }

    @CacheEvict(value = "products", key = "#id")
    public void remove(String id) {
        log.info("Remove Product {}", id);
    }
}
