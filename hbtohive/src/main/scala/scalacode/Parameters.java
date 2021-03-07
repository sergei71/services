package scalacode;

import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;

public class Parameters implements Serializable{

    private String HADOOP_HOME_DIR = "/usr/local/hadoop";

    String filepath;
    String[] paths = {"$.entryitempath", "$.systemprefix", "$.complexoperid", "$.systeminfo", "$.mains",
            "$.dels", "$.singles", "$.objectnames", "$.fields", "$.newfields", "$.objectkeys[%s]"};
    String json;
    java.util.HashMap<String,String[]> props;
    String entryitempath;
    String systemprefix;
    String  complexoperidpath;
    String complexoperid;
    String entryname;
    String  seq;
    String [] systeminfo;
    String[] singles;
    String[] objectnames;
    java.util.HashMap<String, String> mainkeys;
    String[] mains;
    java.util.HashMap<String,String> delflags;
    String[] dels;
    java.util.HashMap<String,String[]> objectkeys;
    java.util.HashMap<String,String>  indices;
    String[] fields;
    String[] newfields;
    StructField[] fieldsstruct;
    StructField[]  newfieldsstruct;
    StructType oldstruct;
    StructType newstruct;

    public Parameters(String path) throws IOException,URISyntaxException{
        json = ReadFromDFS.readHdfs(path);
        props = Tools3.parseJsonConf(json, paths);
        entryitempath = props.get(paths[0])[0];
        systemprefix = props.get(paths[1])[0];
        complexoperidpath = props.get(paths[2])[0];
        complexoperid = "ComplexOperId";
        entryname = "EntryName";
        seq = "Seq";
        systeminfo = props.get(paths[3]);
        singles = props.get(paths[6]);
        objectnames = props.get(paths[7]);
        mainkeys = new java.util.HashMap<String, String>();
        mains = props.get(paths[4]);
        for (int i=0;i<objectnames.length;i++) {
            mainkeys.put(objectnames[i], mains[i]);
        }
        delflags = new java.util.HashMap<String, String>();
        dels = props.get(paths[5]);
        for (int i=0;i<objectnames.length;i++) {
            delflags.put(objectnames[i], dels[i]);
        }
        objectkeys = new java.util.HashMap<String, String[]>();
        for (int i=0;i<objectnames.length;i++) {
            String ind = String.valueOf(i);
            String[] arr = props.get(ind);
            objectkeys.put(objectnames[i],arr);
        }

        indices = new java.util.HashMap<String, String>();
        int count = 0;
        for (int i=0;i<systeminfo.length;i++) {
            indices.put(systeminfo[i], String.valueOf(i));
        }
        count += systeminfo.length;
        indices.put("ComplexOperId", String.valueOf(count));
        count += 1;
        indices.put("EntryName", String.valueOf(count));
        count += 1;
        indices.put("Seq", String.valueOf(count));
        count += 1;

        for (int i=0;i<singles.length;i++) {
            indices.put(singles[i], String.valueOf(count + i));
        }
        count += singles.length;
        for (int i=0;i<objectnames.length;i++) {
            String[] keyses = objectkeys.get(objectnames[i]);
            for (int j=0;j<keyses.length;j++) {
                indices.put(keyses[j], String.valueOf(count + j));
            }
            count += keyses.length;
        }

        fields = props.get(paths[8]);
        newfields = props.get(paths[9]);

        fieldsstruct = new StructField[fields.length];
        for (int i=0;i<fieldsstruct.length;i++) {
            fieldsstruct[i] = DataTypes.createStructField(fields[i], DataTypes.StringType, true);
        }

        newfieldsstruct = new StructField[newfields.length];
        for (int i=0;i<newfieldsstruct.length;i++) {
            newfieldsstruct[i] = DataTypes.createStructField(newfields[i], DataTypes.StringType, true);
        }
        oldstruct = DataTypes.createStructType(fieldsstruct);
        newstruct = DataTypes.createStructType(newfieldsstruct);
    }
}
