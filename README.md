## Introduction

This is a repo with the source code of the Kafka Streams workshop. It's designed as an easy way to
use Kafka Streams with minimal setup and dependencies. We'll start implementing a fictional business scenario
and then adding new requirements step by step, from easy usage of Kafka Streams to more elaborated
functionality.

All the exercises will be explained during the workshop. If you are blocked, you can open the following exercise
to see the solution to the previous one. We encourage you to try do the exercises by yourself. It's the best way
to really learn Kafka Streams.

## Software Requirements

To get started you need to have installed following:

* [Java Development Kit 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

Project is built using [Gradle](https://gradle.org/) build tool but it’s using [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) 
so you are not required to install it manually nor you don’t have to be afraid of version clash.

You can use IDE of your choice but [IntelliJ IDEA](https://www.jetbrains.com/idea/) provides so far the best Gradle 
integration even in the free Community version. Import the project as Gradle project get the best developer experience.

## What are you going to do?

You have just been hired by a big retail company as Data Engineer. Ready for the challenge?.

Let's start with the first task!

### Exercise 0: reading, transforming and writing to Kafka

Security department has raised an issue because credit card numbers are being stored in different databases in the
company and that's a huge potential risk. Your first tasks is mitigate that problem. The easiest way is obfuscate 
the credit card number in the `purchase` topic when is copied from the `transaction` topic.  

You have a deadline in tree months and six-figures budget. Unfortunately, this workshop only last 2 hours so forget 
the budget and let's start coding.

0. Open `src/test/java/antonmry/exercise_0/KafkaStreamsIntegrationTest0.java` and investigate how an integration test
is done with Kafka Streams and get familiarised with the test and the format of the of the messages.
1. Open `src/main/java/antonmry/exercise_0/KafkaStreamsApp0.java`
2. Complete with the proper code the places indicated with a TODO comment (except the optional).
3. Test it! We recommend to launch the test using your IDE instead of Gradle so you can do it easily but you can also
do it also from command line:

Windows:
```
./gradlew.bat test --tests KafkaStreamsIntegrationTest0
```
Linux:
```
./gradlew test --tests KafkaStreamsIntegrationTest0
```

Done? Do you have time yet? Try with the optional part: write some unit tests. See the 
[official documentation](https://kafka.apache.org/11/documentation/streams/developer-guide/testing.html). 

### Exercise 1: build a topology with several nodes

Wow! Your managers are quite surprised with how fast you solved the credit card numbers issue. They see the potential of
the Kafka platform and your skills and they want take advantage of both of them. More requirements are coming! 

- A new rewarding system is going to be deployed and it needs information about the purchases to determine the rewards.
It will read this information from a topic `rewards`.
- Some important business people would like to be able to identify purchases patterns in real-time. A new service of 
reporting has been deployed but we need to feed it with the information. It will read from a topic `PurchasePattern`.

Next steps:

0. Open `src/test/java/antonmry/exercise_1/KafkaStreamsIntegrationTest1.java` and investigate how an integration test
is done with Kafka Streams and get familiarised with the test and the format of the of the messages.
1. Open `src/main/java/antonmry/exercise_1/KafkaStreamsApp0.java` and complete with the proper code the places indicated 
with a TODO comment (except the optional).
2. Open `src/main/java/antonmry/model/PurchasePattern.java` and complete with the proper code the places indicated 
with a TODO comment. 
3. Open `src/main/java/antonmry/model/RewardAccumulator.java` and complete with the proper code the places indicated 
with a TODO comment. 
4. Test it! We recommend to launch the test using your IDE instead of Gradle so you can do it easily but you can also
do it also from command line:

Windows:
```
./gradlew.bat test --tests KafkaStreamsIntegrationTest1
```
Linux:
```
./gradlew test --tests KafkaStreamsIntegrationTest1
```

Done? Do you have time yet? Try with the optional part: improve performance changing from JSON to Avro serialization. 
See the [official documentation](https://docs.confluent.io/current/streams/developer-guide/datatypes.html#avro).

### Exercise 2: filtering, branching and adding keys

OPTIONAL: 

```
https://hub.docker.com/r/spotify/kafka/ 
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`docker-machine ip \`docker-machine active\`` --env ADVERTISED_PORT=9092 spotify/kafka
docker run -p 2181:2181 -p 9092:9092  spotify/kafka

./gradlew runExercise0
```


## Acknowledgements

This workshop content and source code has been heavily inspired by `Kafka Streams in Action`. If you really want to 
learn Kafka Streams we encourage you to buy [the book](https://www.manning.com/books/kafka-streams-in-action) and take 
a look to [the Github repo with the source code](https://github.com/bbejeck/kafka-streams-in-action).

## TODO

- [ ] Delete no needed dependencies
- [ ] Update Kafka version
