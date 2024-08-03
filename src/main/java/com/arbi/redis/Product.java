package com.arbi.redis;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.annotation.KeySpace;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@KeySpace("products")
public class Product {

    @Id
    private String id;

    private String name;

    private Long price;
}
