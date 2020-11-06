package com.atguigu.es.entity;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class Product {
    private Long id;//商品唯一标识
    private String title;//商品名称
    private Double price;//商品价格
    private String images;//图片地址
}
