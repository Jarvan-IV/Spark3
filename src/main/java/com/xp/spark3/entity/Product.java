package com.xp.spark3.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class Product {

    private Integer product_no;
    private String name;
    private Float price;
}
