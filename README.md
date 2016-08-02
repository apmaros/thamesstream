# thamesstream
Example project for Kafka Streams written over a beer near by the Thames. Its purpose is to

## Usage

./bin/kafka-tools.sh kafka-console-consumer \
  --zookeeper zookeeper:2181 \
  --topic clicks-per-region \
  --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


./bin/kafka-tools.sh kafka-console-producer --broker-list kafka:9092 --topic regions --property parse.key=true --property key.separator="--"

alice--asia
bob--america
chao--asia
dave--europe
alice--europe
eve--america
fang--usa


./bin/kafka-tools.sh kafka-console-producer --broker-list kafka:9092 --topic clicks --property parse.key=true --property key.separator="--"

alice--13
bob--4
chao--35
bob--19
dave--56
alice--40
fang--99

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
