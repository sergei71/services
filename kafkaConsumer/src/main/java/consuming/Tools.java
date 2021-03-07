package consuming;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import javax.xml.bind.JAXBElement;
import javax.xml.soap.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

import org.json.JSONObject;
import org.json.JSONArray;
import com.jayway.jsonpath.*;

public class Tools {
    public Tools(){};

    public static void write(String input,List<String> lst) throws java.io.IOException{
        File file = new File(input);
        boolean exists = Files.exists(file.toPath());
        if(exists==false){
            Files.createFile(file.toPath());
        }
        java.nio.file.Files.write(file.toPath(),lst, StandardOpenOption.APPEND);
    }

    public static void write(String input,byte[] lst) throws java.io.IOException{
        File file = new File(input);
        boolean exists = Files.exists(file.toPath());
        if(exists==false){
            Files.createFile(file.toPath());
        }
        java.nio.file.Files.write(file.toPath(),lst, StandardOpenOption.APPEND);
    }


    public static List<String> treeWalk(Element element, String root, String tmp, List<String> arr) {
        //tmp должно изначально иметь значение "/"
        root += "/"+tmp;
        for (int i = 0, size = element.nodeCount(); i < size; i++) {
            Node node = element.node(i);
            String tmp1 = node.getName();
            if(node.getParent()!=null) {
                tmp = node.getParent().getName();
            }
            if(tmp1!=null) {
                tmp1 = root+"/"+tmp+"/"+tmp1;
                tmp1 = tmp1.replaceFirst("/","");
                tmp1 = tmp1.replaceFirst("/","");
                arr.add(tmp1);
            }
            if (node instanceof Element) {
                treeWalk((Element) node,root,tmp,arr);
            }
            else {
            }
        }
        return arr;
    }

    public static JSONObject makeBranch(JSONObject js,String[]path){
        JSONObject joc = null;
        JSONArray jac = null;
        int count = 0;
        if(js.isNull(path[0])) {
            if (path[0].contains("[")) {
                js.put(path[0].replace("[","").replace("]",""), new JSONArray());
                jac = js.getJSONArray(path[0].replace("[","").replace("]",""));
            } else {
                js.put(path[0], new JSONObject());
                joc = js.getJSONObject(path[0]);
            }
        }
        else{
            if (path[0].contains("[")) {
                jac = js.getJSONArray(path[0]);
            } else {
                joc = js.getJSONObject(path[0]);
            }
        }
        for(int i=1;i< path.length-1;i++){
            if(path[i-1].contains("[")){
                if(jac.isNull(path.length-1)) {
                    if (path[i].contains("[")) {
                        jac.put(new JSONArray());
                        jac = jac.getJSONArray(jac.length() - 1);
                    } else {
                        jac.put(new JSONObject());
                        jac = joc.getJSONArray(path[i - 1].replace("[","").replace("]",""));
                    }
                }
                else{
                    if (path[i].contains("[]")) {
                        jac = jac.getJSONArray(jac.length() - 1);
                    } else {
                        jac = joc.getJSONArray(path[i - 1].replaceAll("\\[\\]",""));
                    }
                }
            }
            else {
                if(joc.isNull(path[i])) {
                    if (path[i].contains("[")) {
                        joc.put(path[i].replace("[","").replace("]",""), new JSONArray());
                        jac = joc.getJSONArray(path[i].replace("[","").replace("]",""));
                    } else {
                        joc.put(path[i].replace("[","").replace("]",""), new JSONObject());
                        joc = joc.getJSONObject(path[i].replace("[","").replace("]",""));
                    }
                }
                else{
                    if (path[i].contains("[")) {
                        jac = joc.getJSONArray(path[i].replace("[","").replace("]",""));
                    } else {
                        joc = joc.getJSONObject(path[i].replace("[","").replace("]",""));
                    }
                }
            }
        }
        if (path[path.length-2].contains("[")) {
            if(jac.isNull(path.length-1)) {
                if (path[path.length - 1].contains("[")) {
                    jac.put(new JSONArray());
                } else {
                    jac.put(new JSONObject());
                }
            }
            else{}
        }
        else {
            if(joc.isNull(path[path.length-1])) {
                if (path[path.length - 1].contains("[")) {
                    joc.put(path[path.length - 1].replace("[","").replace("]",""), new JSONArray());
                } else {
                    joc.put(path[path.length - 1].replace("[","").replace("]",""), "");
                }
            }
            else {}
        }
        return  js;
    }

    public static JSONObject makeJson(String[] paths){
        JSONObject jsonobj = new JSONObject();
        for(int i=0;i<paths.length;i++){
            String[] levels = paths[i].split("\\.");
            if(levels[levels.length-1].contains("[")) {
                String key=levels[levels.length-1].replace("[","").replace("]","");
                if (levels.length == 1) {
                    jsonobj.put(key,new JSONArray());
                }
                else {
                    jsonobj = makeBranch(jsonobj,levels);
                }
            }
            else {
                if (levels.length == 1) {
                    jsonobj.put(levels[levels.length-1].replace("[","").replace("]",""),"");
                }
                else {
                    jsonobj = makeBranch(jsonobj,levels);
                }
            }
        }
        return jsonobj;
    }


    public static void constrSoapUpdateEntry(String xml) throws SOAPException, DocumentException {
        xml = xml.replace(" xmlns=\"http://xxxxxxxxxxxxxxxxxxxxxxxx","");
        SOAPConnectionFactory scf;
        SOAPConnection sc ;
        MessageFactory mf ;
        try {
            SAXReader reader = new SAXReader();
            Document doc = reader.read(new ByteArrayInputStream(Bytes.toBytes(xml)));
            scf = SOAPConnectionFactory.newInstance();
            sc = scf.createConnection();
            mf = MessageFactory.newInstance();
            SOAPMessage msg = mf.createMessage();
            SOAPPart soapPart = msg.getSOAPPart();
            SOAPEnvelope envelope = soapPart.getEnvelope();
            SOAPHeader header = envelope.getHeader();
            header.setPrefix("soapenv");
            envelope.removeNamespaceDeclaration("SOAP-ENV");
            envelope.setPrefix("soapenv");
            envelope.addNamespaceDeclaration("soapenv", "http://schemas.xmlsoap.org/soap/envelope/");
            envelope.addNamespaceDeclaration("con", "http://xxxxxxxxxxxxxxxx");
            SOAPBody body = envelope.getBody();
            body.setPrefix("soapenv");
            SOAPElement updateEntryRequest = body.addChildElement("UpdateEntryRequest", "con");
            if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo") != null) {
                SOAPElement systemInfo = updateEntryRequest.addChildElement("SystemInfo", "con");
                SOAPElement from = systemInfo.addChildElement("From", "con").
                        addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/From").getText());
                SOAPElement to = systemInfo.addChildElement("To", "con").
                        addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/To").getText());
                if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/MessageId") != null) {
                    SOAPElement messageId = systemInfo.addChildElement("MessageId", "con").
                            addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/MessageId").getText());
                }
                if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/SrcMessageId") != null) {
                    SOAPElement srcmessageId = systemInfo.addChildElement("SrcMessageId", "con").
                            addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/SrcMessageId").getText());
                }
                if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/SentDateTime") != null) {
                    SOAPElement sentdatetime = systemInfo.addChildElement("SentDateTime", "con").
                            addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/SentDateTime").getText());
                }
                if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/Priority") != null) {
                    SOAPElement priority = systemInfo.addChildElement("Priority", "con").
                            addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/Priority").getText());
                }
                if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/ReqSeq") != null) {
                    SOAPElement reqseq = systemInfo.addChildElement("ReqSeq", "con").
                            addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/ReqSeq").getText());
                }
                if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/ExchKey") != null) {
                    SOAPElement exchkey = systemInfo.addChildElement("ExchKey", "con").
                            addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/ExchKey").getText());
                }
                if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/SessKey") != null) {
                    SOAPElement sesskey = systemInfo.addChildElement("SessKey", "con").
                            addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/SessKey").getText());
                }
                if (doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/Digest") != null) {
                    SOAPElement digest = systemInfo.addChildElement("Digest", "con").
                            addTextNode(doc.selectSingleNode("/UpdateEntryRequest/SystemInfo/Digest").getText());
                }
            }
            if (doc.selectSingleNode("/UpdateEntryRequest/ComplexOperId") != null) {
                SOAPElement complexoperid = updateEntryRequest.addChildElement("ComplexOperId", "con").
                        addTextNode(doc.selectSingleNode("/UpdateEntryRequest/ComplexOperId").getText());
            }
            if (doc.selectSingleNode("/UpdateEntryRequest/EntryItem") != null) {
                List<Node> nodes = doc.selectNodes("/UpdateEntryRequest/EntryItem");
                List<Element> elems = new ArrayList<>();
                for (Node n : nodes) {
                    SOAPElement elem = updateEntryRequest.addChildElement("EntryItem", "con");
                    elem.addChildElement("EntryName", "con").
                            addTextNode(n.selectSingleNode("EntryName").getText());
                    if (n.selectSingleNode("Seq") != null) {
                        elem.addChildElement("Seq", "con").
                                addTextNode(n.selectSingleNode("Seq").getText());
                    }
                    List<Node> nodes1 = n.selectNodes("Attribute");
                    for (Node n1 : nodes1) {
                        SOAPElement attr = elem.addChildElement("Attribute", "con");
                        attr.addChildElement("Name", "con").
                                addTextNode(n1.selectSingleNode("Name").getText());
                        List<Node> values1 = n1.selectNodes("Value");
                        for (Node a1 : values1) {
                            attr.addChildElement("Value", "con").
                                    addTextNode(a1.getText());
                        }
                    }
                    List<Node> nodes2 = n.selectNodes("Object");
                    for (Node n2 : nodes2) {
                        SOAPElement obj = elem.addChildElement("Object", "con");
                        obj.addChildElement("Name", "con").
                                addTextNode(n2.selectSingleNode("Name").getText());
                        List<Node> nodes3 = n2.selectNodes("Attribute");
                        for (Node n3 : nodes3) {
                            SOAPElement attrobj = obj.addChildElement("Attribute", "con");
                            attrobj.addChildElement("Name", "con").
                                    addTextNode(n3.selectSingleNode("Name").getText());
                            List<Node> values2 = n3.selectNodes("Value");
                            for (Node a2 : values2) {
                                attrobj.addChildElement("Value", "con").
                                        addTextNode(a2.getText());
                            }
                        }
                    }
                }
            }
            msg.saveChanges();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            msg.writeTo(baos);
            byte[] b = baos.toByteArray();
            List<Byte>lb1 = new ArrayList<>();
            for(int i=0;i<b.length;i++){
                lb1.add(b[i]);
            }
            String date = Calendar.getInstance().getTime().toString();
            byte[] bytedate = date.getBytes();
            List<Byte>lb2 = new ArrayList<>();
            for(int i=0;i<bytedate.length;i++){
                lb2.add(bytedate[i]);
            }
            byte[] space = "\n".getBytes();
            List<Byte>lb3 = new ArrayList<>();
            for(int i=0;i<space.length;i++){
                lb3.add(space[i]);
            }

            lb2.addAll(lb1);
            lb2.addAll(lb3);
            Byte[] arr = new Byte[lb2.size()];
            arr = lb2.toArray(arr);
            byte[]bytearr = new byte[arr.length];
            for(int i=0;i<arr.length;i++){
                bytearr[i] = arr[i].byteValue();
            }
            write("./soaps",bytearr);
            SOAPMessage response = sc.call(msg, "http://xxxxxx?wsdl");
            response.writeTo(System.out);
            sc.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void constrSoapAddEntry(String xml) throws SOAPException, DocumentException {
        xml = xml.replace(" xmlns=\"http://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","");
        SOAPConnectionFactory scf;
        SOAPConnection sc ;
        MessageFactory mf ;
        try {
            SAXReader reader = new SAXReader();
            Document doc = reader.read(new ByteArrayInputStream(Bytes.toBytes(xml)));
            scf = SOAPConnectionFactory.newInstance();
            sc = scf.createConnection();
            mf = MessageFactory.newInstance();
            SOAPMessage msg = mf.createMessage();
            SOAPPart soapPart = msg.getSOAPPart();
            SOAPEnvelope envelope = soapPart.getEnvelope();
            SOAPHeader header = envelope.getHeader();
            header.setPrefix("soapenv");
            envelope.removeNamespaceDeclaration("SOAP-ENV");
            envelope.setPrefix("soapenv");
            envelope.addNamespaceDeclaration("soapenv", "http://schemas.xmlsoap.org/soap/envelope/");
            envelope.addNamespaceDeclaration("con", "http://xmlns.dit.mos.ru/sudir/itb/connector");
            SOAPBody body = envelope.getBody();
            body.setPrefix("soapenv");
            SOAPElement addEntryRequest = body.addChildElement("AddEntryRequest", "con");
            SOAPElement entryName = addEntryRequest.addChildElement("EntryName", "con").
                    addTextNode(doc.selectSingleNode("/AddEntryRequest/EntryName").getText());
            if (doc.selectSingleNode("/AddEntryRequest/Seq") != null) {
                SOAPElement seq = addEntryRequest.addChildElement("/AddEntryRequest/Seq").
                        addTextNode(doc.selectSingleNode("/AddEntryRequest/Seq").getText());
            }
            List<Node> nodes = doc.selectNodes("/AddEntryRequest/Attribute");
            for (Node n : nodes) {
                SOAPElement elem = addEntryRequest.addChildElement("Attribute", "con");
                elem.addChildElement("Name", "con").
                        addTextNode(n.selectSingleNode("Name").getText());
                elem.addChildElement("Value", "con").
                        addTextNode(n.selectSingleNode("Value").getText());
            }
            if (doc.selectSingleNode("/AddEntryRequest/Object") != null) {
                List<Node> nodes1 = doc.selectNodes("/AddEntryRequest/Object");
                for (Node n1 : nodes1) {
                    SOAPElement obj = addEntryRequest.addChildElement("Object", "con");
                    obj.addChildElement("Name", "con").
                            addTextNode(n1.selectSingleNode("Name").getText());
                    List<Node> nodes2 = doc.selectNodes("/AddEntryRequest/Object/Attribute");
                    for (Node n2 : nodes2) {
                        SOAPElement attrobj = obj.addChildElement("Attribute", "con");
                        attrobj.addChildElement("Name", "con").
                                addTextNode(n2.selectSingleNode("Name").getText());
                        attrobj.addChildElement("Value", "con").
                                addTextNode(n2.selectSingleNode("Value").getText());
                    }
                }
            }
            msg.saveChanges();
            SOAPMessage response = sc.call(msg, "http://xxxxxxxxxxxxxxxxxxxxxxx?wsdl");
            sc.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void constrSoapAddEntryEx(String xml) throws SOAPException, DocumentException {
        xml = xml.replace(" xmlns=\"http://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","");
        SOAPConnectionFactory scf;
        SOAPConnection sc ;
        MessageFactory mf ;
        try {
            SAXReader reader = new SAXReader();
            Document doc = reader.read(new ByteArrayInputStream(Bytes.toBytes(xml)));
            scf = SOAPConnectionFactory.newInstance();
            sc = scf.createConnection();
            mf = MessageFactory.newInstance();
            SOAPMessage msg = mf.createMessage();
            // Компоненты сообщения
            SOAPPart soapPart = msg.getSOAPPart();
            SOAPEnvelope envelope = soapPart.getEnvelope();
            SOAPHeader header = envelope.getHeader();
            header.setPrefix("soapenv");
            envelope.removeNamespaceDeclaration("SOAP-ENV");
            envelope.setPrefix("soapenv");
            envelope.addNamespaceDeclaration("soapenv", "http://schemas.xmlsoap.org/soap/envelope/");
            envelope.addNamespaceDeclaration("con", "http://xxxxxxxxxxxxxxxxxxxxxxxxxx");
            SOAPBody body = envelope.getBody();
            body.setPrefix("soapenv");
            SOAPElement addEntryRequest = body.addChildElement("AddEntryExRequest", "con");
            if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo") != null) {
                SOAPElement systemInfo = addEntryRequest.addChildElement("SystemInfo", "con");
                SOAPElement from = systemInfo.addChildElement("From", "con").
                        addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/From").getText());
                SOAPElement to = systemInfo.addChildElement("To", "con").
                        addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/To").getText());
                if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo/MessageId") != null) {
                    SOAPElement messageId = systemInfo.addChildElement("MessageId", "con").
                            addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/MessageId").getText());
                }
                if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo/SrcMessageId") != null) {
                    SOAPElement srcmessageId = systemInfo.addChildElement("SrcMessageId", "con").
                            addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/SrcMessageId").getText());
                }
                if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo/SentDateTime") != null) {
                    SOAPElement sentdatetime = systemInfo.addChildElement("SentDateTime", "con").
                            addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/SentDateTime").getText());
                }
                if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo/Priority") != null) {
                    SOAPElement priority = systemInfo.addChildElement("Priority", "con").
                            addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/Priority").getText());
                }
                if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo/ReqSeq") != null) {
                    SOAPElement reqseq = systemInfo.addChildElement("ReqSeq", "con").
                            addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/ReqSeq").getText());
                }
                if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo/ExchKey") != null) {
                    SOAPElement exchkey = systemInfo.addChildElement("ExchKey", "con").
                            addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/ExchKey").getText());
                }
                if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo/SessKey") != null) {
                    SOAPElement sesskey = systemInfo.addChildElement("SessKey", "con").
                            addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/SessKey").getText());
                }
                if (doc.selectSingleNode("/AddEntryExRequest/SystemInfo/Digest") != null) {
                    SOAPElement digest = systemInfo.addChildElement("Digest", "con").
                            addTextNode(doc.selectSingleNode("/AddEntryExRequest/SystemInfo/Digest").getText());
                }
            }
            if (doc.selectSingleNode("/AddEntryExRequest/ComplexOperId") != null) {
                SOAPElement complexoperid = addEntryRequest.addChildElement("ComplexOperId", "con").
                        addTextNode(doc.selectSingleNode("/AddEntryExRequest/ComplexOperId").getText());
            }
            if (doc.selectSingleNode("/AddEntryExRequest/EntryItem") != null) {
                List<Node> nodes = doc.selectNodes("/AddEntryExRequest/EntryItem");
                for (Node n : nodes) {
                    SOAPElement elem = addEntryRequest.addChildElement("EntryItem", "con");
                    elem.addChildElement("EntryName", "con").
                            addTextNode(n.selectSingleNode("EntryName").getText());
                    if (n.selectSingleNode("Seq") != null) {
                        elem.addChildElement("Seq", "con").
                                addTextNode(n.selectSingleNode("Seq").getText());
                    }
                    List<Node> nodes1 = n.selectNodes("Attribute");
                    for (Node n1 : nodes1) {
                        SOAPElement attr = elem.addChildElement("Attribute", "con");
                        attr.addChildElement("Name", "con").
                                addTextNode(n1.selectSingleNode("Name").getText());
                        List<Node> values1 = n1.selectNodes("Value");
                        for (Node a1 : values1) {
                            attr.addChildElement("Value", "con").
                                    addTextNode(a1.getText());
                        }
                    }
                    List<Node> nodes2 = n.selectNodes("Object");
                    for (Node n2 : nodes2) {
                        SOAPElement obj = elem.addChildElement("Object", "con");
                        obj.addChildElement("Name", "con").
                                addTextNode(n2.selectSingleNode("Name").getText());
                        List<Node> nodes3 = n2.selectNodes("Attribute");
                        for (Node n3 : nodes3) {
                            SOAPElement attrobj = obj.addChildElement("Attribute", "con");
                            attrobj.addChildElement("Name", "con").
                                    addTextNode(n3.selectSingleNode("Name").getText());
                            List<Node> values2 = n3.selectNodes("Value");
                            for (Node a2 : values2) {
                                attrobj.addChildElement("Value", "con").
                                        addTextNode(a2.getText());
                            }
                        }
                    }
                }
            }
            msg.saveChanges();
            SOAPMessage response = sc.call(msg, "http://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx?wsdl");
            sc.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void sendSoap(String xml) throws DocumentException, SOAPException {
        if(xml.contains("AddEntryExRequest")){
            constrSoapAddEntryEx(xml);
        }
        else if(xml.contains("UpdateEntryRequest")){
            constrSoapUpdateEntry(xml);
        }
        else  if(xml.contains("AddEntryRequest")){
            constrSoapAddEntry(xml);
        }
    }
}
