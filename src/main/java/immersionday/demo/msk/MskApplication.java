package immersionday.demo.msk;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@SpringBootApplication
@RestController
public class MskApplication {

	@Value("${msk.Bootstrap}")
	public String strBootstrap;
	@Value("${msk.ZooKeeper}")
	public String strZooKeeper;

	@RequestMapping(value="demo",method = RequestMethod.GET)
	public String demo(){

	//	String zoostr=strBootstrap;
	//	String boostr=strZooKeeper;
		String basehtml="<html>\n" +
				"<head>\n" +
				"\t<title>MSK Demo</title>\n" +
				"<style>\n" +
				"\t\t.center {\n" +
				"\t\t  margin: auto;\n" +
				"\t\t  width: 60%;\n" +
				"\t\t  border: 1px solid #A9A9A9;\n" +
				"\t\t  padding: 6px;\n" +
				"\t\t}\n" +
				"\t\th1 {\n" +
				"\t\t  text-align: center;\n" +
				"\t\t  text-transform: uppercase;\n" +
				"\t\t  color: #000000;\n" +
				"\t\t}\n" +
				"\t</style>"+
				"</head>\n" +
				"<body>\n" +
				"\t<div >\n" +
				"\t\t<h1>MSK Immersion Day Demo</h1>\n" +
				"\t</div>\n" +
				"\t<div class=\"center\">\n" +
				"        <p>Config:</p>\n" +
				"            <form action=\"/setconfig\" method=\"get\">\n" +
				"            \tBootstrap Servers:<input type=\"text\" name=\"BootstrapName\" value="+ strBootstrap +">\n" +
				"            \t<br>\n" +
				"            \tZooKeeper Connection:<input type=\"text\" name=\"ZooKeeperConn\" value="+ strBootstrap + ">\n" +
				"            \t<br>\n" +
				"               <!-- <button type=\"submit\">SetConfig</button> -->\n" +
				"            </form>\n" +
				"\t\t\t<br>\n" +
				"</div>"+
				"\t<div class=\"center\">\n" +
				"        <p>Create Topic:</p>\n" +
				"            <form action=\"/createtopic\" method=\"get\">\n" +
				"            \tTopic Name:<input type=\"text\" name=\"topicname\">\n" +
				"            \t<br>\n" +
				"                <button type=\"submit\">CreateTopic</button>\n" +
				"            </form>\n" +
				"\t\t\t<br>\n" +
				"    </div>\n" +
				"    <div class=\"center\">\n" +
				"        <p>Producer:</p>\n" +
				"            <form action=\"/produce\" method=\"get\">\n" +
				"            \tTopic:<input type=\"text\" name=\"TopicName\">\n" +
				"            \t<br>\n" +
				"            \tMessage:<input type=\"text\" name=\"ProduceMsg\">\n" +
				"            \t<br>\n" +
				"                <button type=\"submit\">Produce</button>\n" +
				"            </form>\n" +
				"\t\t\t<br>\n" +
				"    </div>\n" +
				"    <div class=\"center\">\n" +
				"        <p>Consumer:</p>\n" +
				"            <form action=\"/consume\" method=\"get\">\n" +
				"            \tTopic:<input type=\"text\" name=\"consumeTopic\">\n" +
				"            \t<br>\n" +
				"                <button type=\"submit\">Consume</button>\n" +
				"            </form>\n" +
				"\t\t\t<br>\n" +
				"    </div>\n" +
				"\t\n" +
				"</body>\n" +
				"</html>";
		return basehtml;
	}

	@RequestMapping(value="createtopic",method = RequestMethod.GET)
	public String createtopic( String topicname) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "b-2.msk-demo.1qmq40.c3.kafka.cn-northwest-1.amazonaws.com.cn:9094,b-1.msk-demo.1qmq40.c3.kafka.cn-northwest-1.amazonaws.com.cn:9094");
		properties.put("group.id", "mskdemo");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("auto.offset.reset", "earliest");
		properties.put("session.timeout.ms", "30000");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// properties.put("topic", "kafka-demo");
		properties.put("security.protocol", "SSL");
		properties.put("ssl.truststore.location", "/etc/pki/ca-trust/extracted/java/cacerts");

		//String strtopic=
		String retStr;
		retStr="<html><body>create topic ok!</body></html>";
		try (AdminClient client = AdminClient.create(properties)) {
			CreateTopicsResult result = client.createTopics(Arrays.asList(
					new NewTopic(topicname, 1, (short) 1)
			));
			try {
				result.all().get();
			} catch ( InterruptedException | ExecutionException e ) {
				retStr= "<html><body>"+e.getMessage().toString()+"</body></html>";
				throw new IllegalStateException(e);

			}
		}
		String retbtnStr="<br><form action=\"/demo\" method=\"get\">\n" +
				"<button type=\"submit\">Return</button>\n" +
				"</form>";
		retStr=retStr+retbtnStr;
		return retStr;
	}
	@RequestMapping(value="produce",method = RequestMethod.GET)
	public String produce(String TopicName,String ProduceMsg) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "b-2.msk-demo.1qmq40.c3.kafka.cn-northwest-1.amazonaws.com.cn:9094,b-1.msk-demo.1qmq40.c3.kafka.cn-northwest-1.amazonaws.com.cn:9094");
		properties.put("group.id", "mskdemo");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("auto.offset.reset", "earliest");
		properties.put("session.timeout.ms", "30000");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// properties.put("topic", "kafka-demo");
		properties.put("security.protocol", "SSL");
		properties.put("ssl.truststore.location", "/etc/pki/ca-trust/extracted/java/cacerts");

		String topic = "demotopic";
		if(!TopicName.isEmpty()) {
			topic = TopicName;
		}
		Producer<String,String> producer =new KafkaProducer<String,String>(properties);
		String value="demovalue";
		if(!ProduceMsg.isEmpty()) {
			value = ProduceMsg;
		}
		ProducerRecord<String,String> msg=new ProducerRecord<String,String>(topic,value);
		producer.send(msg);

		List<PartitionInfo> partitions=new ArrayList<PartitionInfo>();
		partitions= producer.partitionsFor(topic);
		for(PartitionInfo p:partitions)
		{
			System.out.println(p);
		}
		System.out.println("send message over.");
		//producer.close(100, TimeUnit.MILLISECONDS);
		producer.close();
		String retStr="<html><body>send message over.</body><html>";
		String retbtnStr="<br><form action=\"/demo\" method=\"get\">\n" +
				"<button type=\"submit\">Return</button>\n" +
				"</form>";
		retStr=retStr+retbtnStr;
		return retStr;
	}
	@RequestMapping(value="consume",method = RequestMethod.GET)
	public String consume(String consumeTopic) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "b-2.msk-demo.1qmq40.c3.kafka.cn-northwest-1.amazonaws.com.cn:9094,b-1.msk-demo.1qmq40.c3.kafka.cn-northwest-1.amazonaws.com.cn:9094");
		properties.put("group.id", "mskdemo");
		properties.put("enable.auto.commit","true");
		properties.put("auto.commit.interval.ms","1000");
		properties.put("session.timeout.ms", "30000");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("auto.offset.reset","earliest");

		properties.put("security.protocol", "SSL");
		properties.put("ssl.truststore.location", "/etc/pki/ca-trust/extracted/java/cacerts");

		String strtopic="demotopic";
		if(!consumeTopic.isEmpty())
		{
			strtopic=consumeTopic;
		}

		String retStr="";
		Consumer<String,String> myconsumer=new KafkaConsumer<String, String>(properties);
		myconsumer.subscribe(Collections.singletonList(strtopic));

		ConsumerRecords<String,String> records=myconsumer.poll(Duration.ofSeconds(5));

		for(ConsumerRecord<String,String> record:records)
		{
			System.out.println(record.value());
			//System.out.println(record);
			retStr=retStr+record.value()+"<br>";
		}
		String retbtnStr="<br><form action=\"/demo\" method=\"get\">\n" +
				"<button type=\"submit\">Return</button>\n" +
				"</form>";
		retStr=retStr+retbtnStr;
		return retStr;
	}
	public static void main(String[] args) {
		SpringApplication.run(MskApplication.class, args);
	}

}

