University project realized for Big Data class with my classmate [Massimiliano Bruni](https://github.com/Maxinho96).

It aims to simulate a real scenario of a precision farming IoT (Internet of Things) application : in a hazelnut orchards field there are many sensors constantly sending data (for this project we had just weather and ground) and a server, which receives all data, stores them in a NoSQL database and performs both near-real-time and batch analysis. The results of this computation could support decisions of agronomists or trigger automated actions on the field.

It's just a pet project but it makes use of some cool technology like: Docker and docker-compose for the environment, MongoDB in a replica set configuration, Kafka with Zookeeper as a reliable and easily scalable message broker, Spring Boot framework for the applications, Spark for batch analysis and Spark Streaming for near-real-time analysis.
