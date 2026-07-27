# RedisStreams

## 프로젝트 개요
Spring Boot 기반으로 Redis Streams의 Producer/Consumer 패턴을 학습하는 토이 프로젝트

## 구조
* RedisStreamKeys: 스트림 키, Consumer 그룹 이름 등 공통 상수
* GroupCreator: 앱 시작 시 Consumer 그룹 생성 (이미 존재하는 경우인 BUSYGROUP만 무시하고, 그 외 예외는 전파)
* StreamListenerConfig: Consumer 등록 및 리스닝 시작
* StreamProducer: 지정한 시간마다 스트림에 XADD (MAXLEN 트리밍 적용)
* StreamConsumer: 메시지 수신(소비-처리) 후 로그 출력, XACK 처리
* PendingMessageRecoverer: 주기적으로 PEL을 확인해 오래 미처리된 메시지를 복구 (XPENDING → XCLAIM → XACK)

## Redis Streams
Redis Docs: https://redis.io/docs/latest/develop/data-types/streams/

Redis는 스트림이 무한정으로 확장되는 것을 막기 위한 트리밍 전략과 여러가지 소비 전략(XREAD, XREADGROUP, XRANGE)을 제공한다. 

### Redis monitor 확인
redis cli에서 monitor 명령어 입력

아래는 초기 버전(단일 Consumer, ACK 처리 X, MAXLEN 트리밍 X) 모니터링 로그이다.
```
# redis-cli
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

* XACK 호출을 하지 않아, PEL(Pending Entries List)에 메시지가 쌓이고 있다.

## MAXLEN 트리밍 전략
스트림이 무한정 커지는 것을 막기 위해 `StreamProducer`에서 XADD 시 `MAXLEN ~ 1000` 옵션(근사치 트리밍)을 함께 보낸다.

### Redis monitor 확인
(TODO: XADD ... MAXLEN 명령 monitor 로그 붙여넣기)

## Recovery 전략
### PendingMessageRecoverer.java

메시지를 소비하고 XACK 처리가 되지 않아, Consumer 그룹에서 아직 ACK 되지 않은(PENDING) 메시지 처리

### Recovery 처리 흐름
1. XPENDING: PENDING 상태인 메시지를 가져옴.
2. XCLAIM: PENDING 상태인 메시지의 소유권 변경
3. XACK: 확인 응답

Consumer가 성공적으로 메시지를 처리하면, XACK를 호출하여 해당 메시지가 다시 처리되지 않도록 해야 하며, 해당 메시지에 대한 PEL 항목도 삭제되어 Redis 서버의 메모리가 해제된다.

### Recovery 모니터링
```
1785127502.698692 [0 172.17.0.1:37812] "XPENDING" "alarm-stream" "alarm-group" "IDLE" "30000" "-" "+" "100"
1785127502.700634 [0 172.17.0.1:37812] "XCLAIM" "alarm-stream" "alarm-group" "consumer-recovery" "30000" "1785108625453-0" "1785108635445-0" "1785108645449-0" "1785108655440-0" "1785108665443-0" "1785108675445-0" "1785108685445-0" "1785108695449-0" "1785108705441-0" "1785108715441-0" "1785108725445-0" "1785108735446-0" "1785108745438-0" "1785108755441-0" "1785108765448-0" "1785108775442-0" "1785108785442-0" "1785108795442-0" "1785108805443-0" "1785108815443-0" "1785108825440-0" "1785108835448-0" "1785108845444-0" "1785108855445-0" "1785108865439-0" "1785108875449-0" "1785108885449-0" "1785108895448-0" "1785108959451-0" "1785108969052-0" "1785108979044-0" "1785109240223-0" "1785110616534-0" "1785110626118-0"
1785127502.702291 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108625453-0"
1785127502.703586 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108635445-0"
1785127502.705013 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108645449-0"
1785127502.706529 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108655440-0"
1785127502.708094 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108665443-0"
1785127502.710010 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108675445-0"
1785127502.711768 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108685445-0"
1785127502.713066 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108695449-0"
1785127502.714435 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108705441-0"
1785127502.715818 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108715441-0"
1785127502.717202 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108725445-0"
1785127502.718577 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108735446-0"
1785127502.720183 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108745438-0"
1785127502.721572 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108755441-0"
1785127502.723046 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108765448-0"
1785127502.724623 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108775442-0"
1785127502.726375 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108785442-0"
1785127502.727991 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108795442-0"
1785127502.729605 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108805443-0"
1785127502.731169 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108815443-0"
1785127502.732745 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108825440-0"
1785127502.734536 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108835448-0"
1785127502.736229 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108845444-0"
1785127502.737871 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108855445-0"
1785127502.739277 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108865439-0"
1785127502.740741 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108875449-0"
1785127502.742176 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108885449-0"
1785127502.743912 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108895448-0"
1785127502.745227 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108959451-0"
1785127502.746446 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108969052-0"
1785127502.747559 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785108979044-0"
1785127502.748722 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785109240223-0"
1785127502.749765 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785110616534-0"
1785127502.750957 [0 172.17.0.1:37812] "XACK" "alarm-stream" "alarm-group" "1785110626118-0"
```

## Group, Consumer 분리 전략
* 같은 그룹 안에서 Consumer 이름을 나누는 이유
  * 처리량 분산(스케일 아웃): 메시지 양이 Consumer 하나로 감당이 안될 때, 같은 로직을 수행하는 인스턴스를 여러개(consumer-1, consumer-2) 띄워서 그룹이 메시지를 나눠주게 만들 수 있다.
  * 장애 격리/복구 단위 구분 Redis는 Consumer 별로 PEL(Pending Entries List)을 따로 추적하므로, 한 인스턴스가 죽어도 그 Consumer 이름에 물려있던 미처리 메시지만 XCLAIM/XAUTOCLAIM으로 다른 Consumer가 넘겨받을 수 있다. 즉 장애 발생 시 복구 대상을 Consumer 단위로 나누고 싶을 때 분리한다.

처리 로직 자체가 다른 경우는 Consumer 이름이 아니라 그룹 자체를 분리해야 한다. 같은 그룹 안에서는 메시지가 그룹당 한 번만 소비되고 때문에, 로직이 다르면 그룹을 나누어야 한다.

아래는 consumer-1과 consumer-2를 동일한 그룹에 등록했을 때의 소비 로그이다.
```
[consumer-1] {time=2026-07-27T11:13:58.536005400, message=Counter: 0}
[consumer-2] {time=2026-07-27T11:14:08.526432200, message=Counter: 1}
[consumer-2] {time=2026-07-27T11:14:18.527828900, message=Counter: 2}
[consumer-1] {time=2026-07-27T11:14:28.530138800, message=Counter: 3}
[consumer-1] {time=2026-07-27T11:14:38.532134400, message=Counter: 4}
[consumer-2] {time=2026-07-27T11:14:48.534803900, message=Counter: 5}
[consumer-2] {time=2026-07-27T11:14:58.537847100, message=Counter: 6}
[consumer-1] {time=2026-07-27T11:15:08.533009700, message=Counter: 7}
```
특징은 어떤 consumer가 메시지를 받을 지 알 수 없으며, 메시지는 한 번만 소비된다는 것이다.

## 최종 버전(Consumer 다수, ACK 처리, MAXLEN 트리밍) Redis 모니터링 로그
```
# redis-cli
127.0.0.1:6379> monitor
OK
1785130852.830401 [0 172.17.0.1:38734] "XADD" "alarm-stream" "MAXLEN" "~" "1000" "*" "message" "Counter: 0" "time" "2026-07-27T14:40:52.824971200"

1785130852.849405 [0 172.17.0.1:38750] "XREADGROUP" "GROUP" "alarm-group" "consumer-1" "BLOCK" "15000" "COUNT" "10" "STREAMS" "alarm-stream" ">"
1785130852.849541 [0 172.17.0.1:38740] "XREADGROUP" "GROUP" "alarm-group" "consumer-2" "BLOCK" "15000" "COUNT" "10" "STREAMS" "alarm-stream" ">"

1785130852.854293 [0 172.17.0.1:38734] "XACK" "alarm-stream" "alarm-group" "1785130852830-0"
1785130852.863346 [0 172.17.0.1:38762] "XREADGROUP" "GROUP" "alarm-group" "consumer-1" "BLOCK" "15000" "COUNT" "10" "STREAMS" "alarm-stream" ">"
1785130862.391510 [0 172.17.0.1:38734] "XADD" "alarm-stream" "MAXLEN" "~" "1000" "*" "message" "Counter: 1" "time" "2026-07-27T14:41:02.390414900"
1785130862.393712 [0 172.17.0.1:38734] "XACK" "alarm-stream" "alarm-group" "1785130862391-0"

1785130862.402682 [0 172.17.0.1:49038] "XREADGROUP" "GROUP" "alarm-group" "consumer-2" "BLOCK" "15000" "COUNT" "10" "STREAMS" "alarm-stream" ">"
1785130867.966938 [0 172.17.0.1:54516] "XREADGROUP" "GROUP" "alarm-group" "consumer-1" "BLOCK" "15000" "COUNT" "10" "STREAMS" "alarm-stream" ">"
1785130872.397881 [0 172.17.0.1:38734] "XADD" "alarm-stream" "MAXLEN" "~" "1000" "*" "message" "Counter: 2" "time" "2026-07-27T14:41:12.395599700"
1785130872.400246 [0 172.17.0.1:38734] "XACK" "alarm-stream" "alarm-group" "1785130872397-0"
1785130872.410633 [0 172.17.0.1:54518] "XREADGROUP" "GROUP" "alarm-group" "consumer-2" "BLOCK" "15000" "COUNT" "10" "STREAMS" "alarm-stream" ">"
...(생략)
```