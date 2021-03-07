package ru.gex.mdm_proxy.elk_consumer.application.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gex.mdm_proxy.elk_consumer.domain.exception.RemoteServiceException;
import ru.gex.mdm_proxy.elk_consumer.infrastructure.KafkaProducer;
import ru.mos.dit.xmlns.sudir.itb.connector.AddEntryExRequestType;
import ru.mos.dit.xmlns.sudir.itb.connector.EntryType;
import ru.mos.dit.xmlns.sudir.itb.connector.UpdateEntryRequestType;

import javax.xml.bind.JAXB;
import javax.xml.bind.JAXBElement;
//import java.io.StringWriter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

@org.springframework.stereotype.Service
public class Service {

    private final static Logger log = LoggerFactory.getLogger(Service.class);
    private final static String pathname = "./inputs";
    

    @Autowired
    KafkaProducer producer;

   
    public void sendEntryType(JAXBElement<EntryType> jaxbObject) throws RemoteServiceException, IOException {
        String xml = jaxbToString(jaxbObject);
        log.debug("Sending xml string: {}", xml);
        producer.send(xml);
    }

    public void sendAddEntryExRequestType (JAXBElement<AddEntryExRequestType> jaxbObject) throws RemoteServiceException,
    IOException{
        String xml = jaxbToString(jaxbObject);
        log.debug("Sending xml string: {}", xml);
        producer.send(xml);
    }

    public void sendUpdateEntryResponseType(JAXBElement<UpdateEntryRequestType> jaxbObject) throws RemoteServiceException,
    IOException{
        String xml = jaxbToString(jaxbObject);
        log.debug("Sending xml string: {}", xml);
        producer.send(xml);
    }

    private String jaxbToString(Object jaxbObject) {
        StringWriter sw = new StringWriter();
        JAXB.marshal(jaxbObject, sw);
        String xmlString = sw.toString();
        return xmlString;
    }

}
