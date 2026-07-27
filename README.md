# RedisStreams

## Redis Streams

Redis Docs: https://redis.io/docs/latest/develop/data-types/streams/

스트림이 무한정으로 확장되는 것을 막기 위한 트리밍 전략과 여러가지 소비 전략(XREAD, XREADGROUP, XRANGE)을 제공한다. 

### Redis monitor 확인
redis cli에서 monitor 명령어 입력


```shell
127.0.0.1:6379> monitor
OK
1785110616.496502 [0 172.17.0.1:37152] "HELLO" "3"
1785110616.514385 [0 172.17.0.1:37152] "CLIENT" "SETINFO" "lib-name" "Lettuce"
1785110616.514405 [0 172.17.0.1:37152] "CLIENT" "SETINFO" "lib-ver" "6.8.2.RELEASE/34f8700"
1785110616.534659 [0 172.17.0.1:37152] "XADD" "alarm-stream" "*" "time" "2026-07-27T09:03:36.118653800" "message" "Counter: 0"
1785110616.534677 [0 172.17.0.1:37152] "XGROUP" "CREATE" "alarm-stream" "alarm-group" "$" "MKSTREAM"
1785110616.558916 [0 172.17.0.1:37156] "HELLO" "3"
1785110616.561473 [0 172.17.0.1:37156] "CLIENT" "SETINFO" "lib-name" "Lettuce"
1785110616.561483 [0 172.17.0.1:37156] "CLIENT" "SETINFO" "lib-ver" "6.8.2.RELEASE/34f8700"
1785110616.565703 [0 172.17.0.1:37156] "XREADGROUP" "GROUP" "alarm-group" "consumer-1" "BLOCK" "15000" "COUNT" "10" "STREAMS" "alarm-stream" ">"
```

* 1785110616.496502 [0 172.17.0.1:37152]
  * DB 번호: 0
  * 클라이언트: 172.17.0.1:37152

* "XADD" "alarm-stream" "*" "time" "2026-07-27T09:03:36.118653800" "message" "Counter: 0"
  * **XADD** 명령을 사용하여
  * **alarm-stream** 스트림에
  * Redis가 자동 ID(**`*`**)를 생성하여
  * message(**Counter: 0**), time(**2026-07-27T09:03:36.118653800**) 필드를 가진 레코드를 저장했다.

* "XREADGROUP" "GROUP" "alarm-group" "consumer-1" "BLOCK" "15000" "COUNT" "10" "STREAMS" "alarm-stream" ">"
  * Consumer 그룹: alarm-group
  * Consumer 이름: consumer-1
  * "BLOCK" "15000": 최대 15초 동안 기다려라
  * "COUNT" "10": 최대 10개
  * "STREAMS" "alarm-stream" ">": 해당 Counsumer 그룹이 아직 받지 않은 새로운 메시지만 읽어라.