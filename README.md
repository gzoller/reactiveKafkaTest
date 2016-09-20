This project is to exercise Reactive Kafka for our use case.

NOTE:  Running this project requires Docker and the gzoller/world image built from the kafka_redis_dynamo branch of the world repo.

Run scripts were built on a MacBook Pro using a docker-machine install of Docker.

This is just a test project--it's stupid-primitive in many places.  Ignore the Producer stuff--it's ugly and it works.  It's the consumer that has the problems.


RESULTS:

This project fails to run under load.  The work happens in RunTest.scala.  When run you get a lot of "dead letter" errors.