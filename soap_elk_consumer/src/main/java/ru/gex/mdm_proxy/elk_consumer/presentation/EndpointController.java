package ru.gex.mdm_proxy.elk_consumer.presentation;

import java.io.*;
import java.nio.file.*;
import java.util.List;

import org.springframework.core.io.Resource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMethod;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;
import ru.gex.mdm_proxy.elk_consumer.application.service.Service;
import ru.gex.mdm_proxy.elk_consumer.domain.exception.RemoteServiceException;
import ru.mos.dit.xmlns.sudir.itb.connector.*;

import javax.xml.bind.JAXBElement;

import static ru.gex.mdm_proxy.elk_consumer.domain.Constant.*;

import org.springframework.core.io.ClassPathResource;

@RestController
@Endpoint
public class EndpointController {

    private final static Logger log = LoggerFactory.getLogger(EndpointController.class);
    private final static String path = "./files/xxxxxxxxxxxxxxxxxxxxx.wsdl.xml";

    @Autowired
    Service service;

    private final ObjectFactory factory = new ObjectFactory();

    private ResponseType createServiceErrorResponse() {
        ResponseType responseType = new ResponseType();
        responseType.setResponseCode(SERVICE_ERROR_CODE);
        responseType.setResponseDescription(SERVICE_ERROR_DESCRIPTION);

        return responseType;
    }

    @RequestMapping(value="/getwsdl",method=RequestMethod.GET)
    public String getWsdl(){
        FileInputStream fin=null;
        String str= "";
        try{
            File file = new File(path);
            Path path1 = file.toPath();
            List<String> lst = Files.readAllLines(path1);
            for(String s:lst){
                str+=s;
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return str;
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "AddEntryRequest")
    @ResponsePayload
    public JAXBElement<ResponseType> addEntry(@RequestPayload JAXBElement<EntryType> request) throws IOException {

        ResponseType response;

        try {
            service.sendEntryType(request);
            response = createOkResponse();
        } catch (RemoteServiceException e) {
            response = createServiceErrorResponse();
        }

        return factory.createAddEntryResponse(response);
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "AddEntryExRequest")
    @ResponsePayload
    public JAXBElement<AddEntryExResponseType> addEntryEx(@RequestPayload JAXBElement<AddEntryExRequestType> request)
    throws  IOException{

        AddEntryExResponseType response = new AddEntryExResponseType();

        try {
            service.sendAddEntryExRequestType(request);
            response.setResponse(createOkResponse());
        } catch (RemoteServiceException e) {
            response.setResponse(createServiceErrorResponse());
        }

        return factory.createAddEntryExResponse(response);
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "DeleteEntryRequest")
    @ResponsePayload
    public JAXBElement<DeleteEntryResponseType> deleteEntry(@RequestPayload JAXBElement<DeleteEntryRequestType> request)
    throws  IOException{
        DeleteEntryResponseType response = new DeleteEntryResponseType();
        response.setResponse(createNotSupportedResponse());

        return factory.createDeleteEntryResponse(response);
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "FindEntryRequest")
    @ResponsePayload
    public JAXBElement<FindEntryResponseType> findEntry(@RequestPayload JAXBElement<FindEntryRequestType> request)
    throws  IOException{
        FindEntryResponseType response = new FindEntryResponseType();
        response.setResponse(createNotSupportedResponse());

        return factory.createFindEntryResponse(response);
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "GetNextEntriesRequest")
    @ResponsePayload
    public JAXBElement<GetNextEntriesResponseType> getNextEntries(@RequestPayload JAXBElement<GetNextEntriesRequestType> request)
    throws IOException{
        GetNextEntriesResponseType response = new GetNextEntriesResponseType();
        response.setResponse(createNotSupportedResponse());

        return factory.createGetNextEntriesResponse(response);
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "GetSessKeyRequest")
    @ResponsePayload
    public JAXBElement<GetSessKeyResponseType> getSessKey(@RequestPayload JAXBElement<GetSessKeyRequestType> request)
    throws IOException{
        GetSessKeyResponseType response = new GetSessKeyResponseType();
        response.setResponse(createNotSupportedResponse());

        return factory.createGetSessKeyResponse(response);
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "SelectEntriesRequest")
    @ResponsePayload
    public JAXBElement<SelectEntriesResponseType> selectEntries(@RequestPayload JAXBElement<SelectEntriesRequestType> request)
    throws IOException{
        SelectEntriesResponseType response = new SelectEntriesResponseType();
        response.setResponse(createNotSupportedResponse());

        return factory.createSelectEntriesResponse(response);
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "UpdateEntryRequest")
    @ResponsePayload
    public JAXBElement<UpdateEntryResponseType> updateEntry(@RequestPayload JAXBElement<UpdateEntryRequestType> request)
    throws IOException{
        UpdateEntryResponseType response = new UpdateEntryResponseType();

        try {
            service.sendUpdateEntryResponseType(request);
            response.setResponse(createOkResponse());
        } catch (RemoteServiceException e) {
            response.setResponse(createServiceErrorResponse());
        }

        return factory.createUpdateEntryResponse(response);
    }



    private ResponseType createNotSupportedResponse() {
        ResponseType responseType = new ResponseType();
        responseType.setResponseCode(NOT_SUPPORT_CODE);
        responseType.setResponseDescription(NOT_SUPPORT_DESCRIPTION);

        return responseType;
    }

    private ResponseType createOkResponse() {
        ResponseType responseType = new ResponseType();
        responseType.setResponseCode(OK_CODE);
        responseType.setResponseDescription(OK_DESCRIPTION);

        return responseType;
    }
}
