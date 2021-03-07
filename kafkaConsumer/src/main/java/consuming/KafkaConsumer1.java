package consuming;
import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.clients.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Calendar;
import java.io.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;

import org.dom4j.DocumentException;

import javax.xml.soap.*;

public class KafkaConsumer1 {
    private Properties props;
    KafkaConsumer<String, String> consumer;
    final int minBatchSize = 200;
    private static final Logger logger = LogManager.getLogger(KafkaConsumer1.class);
    public static final String KAFKA_PROPERTIES="./files/kafka.properties";
    public static final String COMMON_PROPERTIES="./files/common.properties";
    private static Map<TopicPartition,OffsetAndMetadata> currentOffsets;
    public KafkaConsumer1(){
        FileInputStream fis=null;
        //Properties props=new Properties();
        props = new Properties();
        currentOffsets = new HashMap<>();
        try{
            fis=new FileInputStream(KAFKA_PROPERTIES);
            props.load(fis);
            consumer = new KafkaConsumer(props);
            String topic = props.getProperty("topic");
            consumer.subscribe(Arrays.asList(topic));
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException, SOAPException, DocumentException, InterruptedException {
        logger.debug("START.");
        FileInputStream fis=null;
        Properties prop=new Properties();
        String hadoop_home_dir="";
        String principal="";
        String keytabPath="";
        String tbName = "";
        String columnFamily="";
        String column1="";
        String column2="";
        try{
            fis=new FileInputStream(COMMON_PROPERTIES);
            prop.load(fis);
            hadoop_home_dir=prop.getProperty("hadoop_home_dir");
            principal=prop.getProperty("principal");
            keytabPath=prop.getProperty("keytabPath");
            tbName=prop.getProperty("tbName");
            columnFamily=prop.getProperty("columnFamily");
            column1=prop.getProperty("column1");
            column2=prop.getProperty("column2");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        DatumWriter <Soartest> dw = new SpecificDatumWriter<Soartest>(Soartest.class);
        DataFileWriter <Soartest>dfw = new DataFileWriter<Soartest>(dw);
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir",hadoop_home_dir);
        Configuration hbconf = HBaseConfiguration.create();
        hbconf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(hbconf);
        UserGroupInformation.loginUserFromKeytab(principal,keytabPath);
        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(hbconf);
        TableName tableName =  TableName.valueOf(tbName);
        Table table = conn.getTable(tableName);
        HTable htable = (HTable)table;

        KafkaConsumer1 kfc= new KafkaConsumer1();
        kfc.props.setProperty("max.poll.records", "100");

        while (true) {
            ConsumerRecords<String, String> records = kfc.consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                Soartest s = new Soartest();
                s.setRec(record.value());
                long ts = Calendar.getInstance().getTimeInMillis();
                byte[]row1 = Bytes.toBytes(ts);
                Put p1 = new Put(row1);
                byte [] databytes = Bytes.toBytes(columnFamily);
                byte[] q = Bytes.toBytes(column1);
                byte[] value = Bytes.toBytes(record.value());
                p1.addColumn(databytes,q,row1);
                p1.addColumn(databytes,Bytes.toBytes(column2),value);
                table.put(p1);
                Tools.sendSoap(s.getRec().toString());
                currentOffsets.put(new TopicPartition(record.topic(),record.partition()),
                        new OffsetAndMetadata(record.offset()+1,"no metadata"));
                kfc.consumer.commitAsync(currentOffsets,null);

            }
        }
    }
}
