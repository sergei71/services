package ru.gex.mdm_proxy.elk_consumer.infrastructure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.gex.mdm_proxy.elk_consumer.domain.exception.RemoteServiceException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class KafkaProducer {
    private final static Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    volatile KafkaTemplate<String, String> template;

    @Value("${kafka.topic.elk}")
    String topicElk;

    public void send(String data) throws RemoteServiceException {
        sendSync(topicElk, data);
    }

    private void sendSync(String topic, String data) throws RemoteServiceException {
        try {
            log.debug("topic {} data {}", topic, data);
            template.send(topic, data).get(10, TimeUnit.SECONDS);
            template.flush();
        } catch (ExecutionException | TimeoutException | InterruptedException e) {
            throw new RemoteServiceException(e);
        }
    }
}

