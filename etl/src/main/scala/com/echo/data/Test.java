package com.echo.data;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class Test {

  public static void main(String[] args) {

    ZonedDateTime expectedTimestamp = ZonedDateTime
        .of(LocalDateTime.parse("2014-09-08T17:51:04.780"), ZoneId.of("Asia/Shanghai"))
        .withZoneSameInstant(ZoneOffset.UTC);

    System.out.println(expectedTimestamp);


    String date = "2021-02-19T00:45:09.798Z";
    ZonedDateTime parsedDate = ZonedDateTime.parse(date);
    System.out.println(parsedDate);

  }

}
