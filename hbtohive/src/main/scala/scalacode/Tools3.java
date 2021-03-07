package scalacode;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.*;
import java.nio.file.*;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.*;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.dom4j.xpath.DefaultXPath;
import com.jayway.jsonpath.*;
import net.minidev.json.JSONArray;

//реализация findmultipleComplexKey, учтены ключи блока SystemInfo, ComplexOperId, и в EntryItem - EntryName
// доступ к узлам и значениям - на основе API org.dom4j
public class Tools3 {

    public static String getLoginname(String xml,String template){
        String loginname = "";
        xml = xml.replace(" xmlns=\"http://xmlns.dit.mos.ru/sudir/itb/connector\"","");
        xml = xml.trim().replaceFirst("^([\\W]+)<","<");
        SAXReader reader = new SAXReader();
        try{
            Document document = reader.read(new ByteArrayInputStream(xml.getBytes("UTF-8")));
            List<Node> nodes = document.selectNodes("/UpdateEntryRequest/EntryItem");
            for(Node node:nodes){
                if(node.selectSingleNode(template+"Name").getText().equals("LoginName")){
                    loginname = node.selectSingleNode(template+"Value").getText();
                }
            }
        }
        catch (Exception e){

        }
        return loginname;
    }

    public static List<String[]> findmultipleComplexKey (String xml, String entryitempath,
                                                         String systemprefix,
                                                         String [] systeminfo,
                                                         String complexoperid,
                                                         String[] singles,
                                                         Map<String,String[]> objects,
                                                         Map<String ,String> indices) throws IOException,
            URISyntaxException,DocumentException {
        xml = xml.replace(" xmlns=\"http://xmlns.dit.mos.ru/sudir/itb/connector\"","");
        xml = xml.trim().replaceFirst("^([\\W]+)<","<");
        if(xml.contains("AddEntryRequest")){
            entryitempath = entryitempath.replaceAll("UpdateEntryRequest","AddEntryRequest");
            systemprefix = systemprefix.replaceAll("UpdateEntryRequest","AddEntryRequest");
        }
        String objectpath =  entryitempath+"/Object";
        String singlepath = entryitempath+"/Attribute";
        List<String[]> results = new ArrayList<>();
        SAXReader reader = new SAXReader();
        try{
            Document document = reader.read(new ByteArrayInputStream(xml.getBytes("UTF-8")));
            //Document document = reader.read(new ByteArrayInputStream(xml.getBytes()));
            Element element = document.getRootElement();
            Map<String ,String > systemvalues = new HashMap<>();
            String complexoperidval = "";
            for(int i = 0;i<systeminfo.length;i++){
                systemvalues.put(systeminfo[i],"");
            }
            for(int i = 0;i<systeminfo.length;i++){
                String path = systemprefix+systeminfo[i];
                if(document.selectSingleNode(path)!=null){
                    systemvalues.put(systeminfo[i],document.selectSingleNode(path).getText());
                }
            }
            if(document.selectSingleNode(complexoperid)!=null){
                complexoperidval = document.selectSingleNode(complexoperid).getText();
            }
            List<Node> entryitemnodes = document.selectNodes(entryitempath);
            if(entryitemnodes.size()>0) {
                for (int i = 0; i < entryitemnodes.size(); i++) {
                    Node node = entryitemnodes.get(i);
                    String entryname = node.selectSingleNode(entryitempath + "/EntryName").getText();
                    String seq = "";
                    if (node.selectSingleNode(entryitempath + "/Seq") != null) {
                        seq = node.selectSingleNode(entryitempath + "/Seq").getText();
                    }
                    List<Node> objectnodes = getObjectnodes(node, objectpath);
                    List<Node> singlenodes = getSinglenodes(node, singles);
                    Map<String, String> singlevalues = getSingleValues(singlenodes, singlepath, singles, indices);
                    fillingResults(objects, indices, systemvalues, complexoperidval,
                            entryname, seq, singlevalues, objectpath, objectnodes, results);
                }
            }
            else{
                emptyResults(indices,results);
            }
        }
        catch(Exception e){
            emptyResults(indices,results);
        }
        return results;
    }

    //добавление в списка пустых результатов - если вх. строка не соотв. структуре XML
    public static void emptyResults(Map<String ,String> indices, List<String[]> results){
        String[] res = new String[indices.size()];
        for (int j = 0; j < res.length; j++) {
            res[j] = "";
        }

        results.add(res);
    }

    public static List<Node> getSinglenodes(Node node,String singlepath,String[] singles) {
        List<Node> singlenodes = node.selectNodes(singlepath);
        for(Node n:singlenodes){
            System.out.println(n);
        }
        return singlenodes;
    }

    public static List<Node> getSinglenodes(Node node,String[] singles){
        List<Node> singlenodes = new ArrayList<>();
        Element tmpel = (Element) node;
        for (int j = 0; j < tmpel.nodeCount(); j++) {
            if (tmpel.node(j).getName() == "Attribute") {
                Element tmpel1 = (Element) tmpel.node(j);
                for (int k = 0; k < tmpel1.nodeCount(); k++) {
                    Node tmpnode = tmpel1.node(k);
                    if(tmpnode.getName()=="Name") {
                        for (int l = 0; l < singles.length; l++) {
                            if (tmpnode.getText().equals(singles[l])){
                                singlenodes.add(tmpel.node(j));
                            }
                        }
                    }
                }
            }
        }
        return  singlenodes;
    }

    public static Map<String ,String > getSingleValues(List<Node> singlenodes,String singlepath,
                                                       String[] singles,Map<String, String> indices) {
        Map<String, String> singlevalues = new HashMap<>(singles.length);
        for(int j=0;j<singles.length;j++){
            boolean flag = true;
            for(int i=0;i<singlenodes.size();i++){
                Node node = singlenodes.get(i);
                String name = node.selectSingleNode(node.getUniquePath()+"/Name").getText();
                if(name.equals(singles[j])){
                    flag = false;
                    singlevalues.put(singles[j],node.selectSingleNode(node.getUniquePath()+"/Value").getText());
                }
            }
            if(flag){
                singlevalues.put(singles[j],"");
            }
        }
        return singlevalues;
    }

    public static List<Node> getObjectnodes(Node node,String objectpath){
        List<Node> objectnodes = new ArrayList<>();
        if(node.selectNodes(objectpath)!=null) {
            objectnodes = node.selectNodes(objectpath);
        }
        return objectnodes;
    }

    //заполнение существующего списка результатов
    public static void fillingResults(Map<String,String[]> objects, Map<String ,String> indices,
                                      Map<String ,String> systemvalues,
                                      String complexoperidval,
                                      String entryname,
                                      String seq,
                                      Map<String ,String> singlevalues,
                                      String objectpath,
                                      List<Node> objectnodes,
                                      List<String[]> results) throws IOException,URISyntaxException{
        Set<String> keyset = objects.keySet();
        String[] keys = new String [keyset.size()];
        keys = keyset.toArray(keys);
        int quantity = 0;
        for(int i=0;i<keys.length;i++){
            quantity += objects.get(keys[i]).length;
        }
        // учет в длине complexoperidval,entryname и seq
        quantity+=3;
        String[] singlekeys = new String [singlevalues.keySet().size()];
        singlekeys = singlevalues.keySet().toArray(singlekeys);
        String[] systemkeys = new String [systemvalues.keySet().size()];
        systemkeys = systemvalues.keySet().toArray(systemkeys);
        if(objectnodes.size()>0) {
            for (int i = 0; i < objectnodes.size(); i++) {
                String[] res = new String[indices.keySet().size()];
                String[] blank = new String[indices.keySet().size()];
                for (int j = 0; j < singlekeys.length; j++) {
                    int ind = Integer.parseInt(indices.get(singlekeys[j]));
                    res[ind] = singlevalues.get(singlekeys[j]);
                    blank[ind] = singlevalues.get(singlekeys[j]);
                }
                for (int j = 0; j < systemkeys.length; j++) {
                    int ind = Integer.parseInt(indices.get(systemkeys[j]));
                    res[ind] = systemvalues.get(systemkeys[j]);
                    blank[ind] = systemvalues.get(systemkeys[j]);
                }
                int complexind = Integer.parseInt(indices.get("ComplexOperId"));
                res[complexind] = complexoperidval;
                int entrynameind = Integer.parseInt(indices.get("EntryName"));
                res[entrynameind] = entryname;
                int seqind = Integer.parseInt(indices.get("Seq"));
                res[seqind] = seq;

                Node tmpel = objectnodes.get(i);
                String key = tmpel.selectSingleNode(objectpath+"/Name").getText();

                String[] keyses = objects.get(key);
                if (keyses == null) {
                    for (int m = 0; m < blank.length; m++) {
                        if (blank[m] == null) {
                            blank[m] = "";
                        }
                    }
                    results.add(blank);
                    continue;
                }
                List<Node> attrs = tmpel.selectNodes(objectpath+"/Attribute");
                for(int n=0;n<attrs.size();n++){
                    Node attr = attrs.get(n);
                    String name = attr.selectSingleNode(attr.getUniquePath()+"/Name").getText();
                    String ind = indices.get(name);
                    if(ind!=null){
                        res[Integer.parseInt(ind)] = attr.selectSingleNode(attr.getUniquePath()+"/Value").getText();
                    }
                }

                for (int m = 0; m < res.length; m++) {
                    if (res[m] == null) {
                        res[m] = "";
                    }
                }
                results.add(res);
            }
        }
        else{
            String[] res = new String[indices.keySet().size()];
            String[] blank = new String[indices.keySet().size()];
            for (int j = 0; j < singlekeys.length; j++) {
                int ind = Integer.parseInt(indices.get(singlekeys[j]));
                res[ind] = singlevalues.get(singlekeys[j]);
                blank[ind] = singlevalues.get(singlekeys[j]);
            }
            for (int j = 0; j < systemkeys.length; j++) {
                int ind = Integer.parseInt(indices.get(systemkeys[j]));
                res[ind] = systemvalues.get(systemkeys[j]);
                blank[ind] = systemvalues.get(systemkeys[j]);
            }
            int complexind = Integer.parseInt(indices.get("ComplexOperId"));
            res[complexind] = complexoperidval;
            int entrynameind = Integer.parseInt(indices.get("EntryName"));
            res[entrynameind] = entryname;
            int seqind = Integer.parseInt(indices.get("Seq"));
            res[seqind] = seq;
            for (int m = 0; m < res.length; m++) {
                if (res[m] == null) {
                    res[m] = "";
                }
            }
            results.add(res);
        }
    }

    public static String readLocalfile(String filepath) throws IOException{
        File file = new File(filepath);
        List<String> res = Files.readAllLines(file.toPath());
        String str = "";
        for(String s:res){
            str+=s.replace("\n","");
        }
        return str;
    }

    public static void writeLocalFile(String filepath,String data) throws IOException{
        File file = new File(filepath);
        if(Files.exists(file.toPath())==false){
            Files.write(file.toPath(),(data+"\n").getBytes());
        }
        Files.write(file.toPath(),(data+"\n").getBytes(),StandardOpenOption.APPEND);
    }

    public static Map<String,String[]> readLocalfile(String filepath,String[] paths) throws IOException{
        File file = new File(filepath);
        List<String> res = Files.readAllLines(file.toPath());
        String json = "";
        Map<String,String[]> result = new HashMap<>();
        for(String s:res){
            json+=s.replace("\n","").replace(" ","");
        }
        Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
        String js = JsonPath.read(document,"$.number");
        int number = Integer.parseInt(js);
        for(int i=0;i<paths.length-1;i++){
            if(i<3) {
                String str = JsonPath.read(document, paths[i]);
                String[] tmp = new String[1];
                tmp[0] = str;
                result.put(paths[i],tmp);
            }
            else{
                JSONArray arr = JsonPath.read(document, paths[i]);
                String [] tmp = new String [arr.size()];
                for(int j=0;j<arr.size();j++){
                    tmp[j] = arr.get(j).toString();
                }
                result.put(paths[i],tmp);
            }
        }
        for(int i=0;i<number;i++){
            String ind = String.valueOf(i);
            String tmppath = String.format(paths[paths.length-1],ind);
            JSONArray arr = JsonPath.read(document, tmppath);
            String [] tmp = new String [arr.size()];
            for(int j=0;j<arr.size();j++){
                tmp[j] = arr.get(j).toString();
            }
            result.put(ind,tmp);
        }
        return result;
    }

    public static HashMap<String,String[]> parseJsonConf(String json,String[] paths) throws IOException{
        HashMap<String,String[]> result = new HashMap<>();
        Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
        String js = JsonPath.read(document,"$.number");
        int number = Integer.parseInt(js);
        for(int i=0;i<paths.length-1;i++){
            if(i<3) {
                String str = JsonPath.read(document, paths[i]);
                String[] tmp = new String[1];
                tmp[0] = str;
                result.put(paths[i],tmp);
            }
            else{
                JSONArray arr = JsonPath.read(document, paths[i]);
                String [] tmp = new String [arr.size()];
                for(int j=0;j<arr.size();j++){
                    tmp[j] = arr.get(j).toString();
                }
                result.put(paths[i],tmp);
            }
        }
        for(int i=0;i<number;i++){
            String ind = String.valueOf(i);
            String tmppath = String.format(paths[paths.length-1],ind);
            JSONArray arr = JsonPath.read(document, tmppath);
            String [] tmp = new String [arr.size()];
            for(int j=0;j<arr.size();j++){
                tmp[j] = arr.get(j).toString();
            }
            result.put(ind,tmp);
        }
        return result;
    }
}

