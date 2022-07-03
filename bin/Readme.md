# Kafka Commands

## Create topic
```
 bin/kafka-topics.sh --create --topic test-producer --bootstrap-server 68.183.95.195:9092
```
## List all the topics
`
bin/kafka-topics.sh --list --bootstrap-server 68.183.95.195:9092
`

## Describe topic
`
bin/kafka-topics.sh --describe --topic test-producer --bootstrap-server 68.183.95.195:9092
`

## Produce message
`
bin/kafka-console-producer.sh --topic test-producer --bootstrap-server 68.183.95.195:9092
`

## Consume message
`
bin/kafka-console-consumer.sh --topic test-producer --from-beginning --bootstrap-server 68.183.95.195:9092
`

## Delete topic
`
bin/kafka-topics.sh --delete --topic test-producer --bootstrap-server 68.183.95.195:9092
`

## Lab-1 Producer Demo
1. Add the Application Configuration
```java
    public final static String applicationID = "Producer-Demo";
    public final static String bootstrapServers = "broker-1:9092,broker-2:9092,broker-3:9092";
    public final static String topicName = "test-producer";
    public final static int numEvents = 1000;
```
2. Configure the Producer with Producer Configuration 
```java
    Properties props = new Properties();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationID);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
```
3. Create an instance of `KafkaProducer`
```java
    KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
```
4. Send messages to broker
```java
    for (int i = 1; i <= AppConfig.numEvents; i++) {
         message = "Message-" + i;
        producer.send(new ProducerRecord<>(AppConfig.topicName, i, message));
        logger.info("Successfully sent message to Broker:: "+ message);
    }
```
5. Close the Producer instance
```java
    producer.close();
```
