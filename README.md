# Building Event-Driven Applications In Go
This is the repository for the LinkedIn Learning course `Building Event-Driven Applications In Go`. The full course is available from [LinkedIn Learning][lil-course-url].

***

# Kafka Client

https://github.com/confluentinc/confluent-kafka-go


Kafka Infrastructure (Docker)
Build with docker compose
```shell
docker-compose up -d
```
Check running docker containers
```shell
docker-compose ps
```

NOTES:
## Event Sourcing
> chap4/04_01


## Command query responsibility segregation (CQRS)
Great for when you have different write and read workloads (they can be scaled seperately)
> chap4/04_01

## Pub-Sub Pattern with isolated broker
> chap5/05_01