package com.example;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

@SpringBootTest
class DemoApplicationTests {

	public static void main(String[] args) {
		Map map = new HashMap();
		map.put("a","a");
		map.put("b","b");
		System.out.println(map.remove("a"));
	}

}
