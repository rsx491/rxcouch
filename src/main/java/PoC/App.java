package PoC;

import java.io.FileReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.couchbase.client.deps.io.netty.channel.kqueue.KQueue;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

/**
 * Very simplified app.. connects to couchbase, reads a JSON file and insert the documents
 */
public class App {
    private static Properties m_props;
    private static File propFile;
    static private Cluster cluster;
    static private Bucket main;
    
    public static void main(String[] args) throws InterruptedException {

        // Load configuration
        m_props = new Properties();
        propFile = new File("configuration.properties");

        try {
            LoadProperties(propFile);
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
        System.out.println("Loaded properties");

        // Setup couchbase
        String dbClusterList = m_props.getProperty("cluster");
        List<String> nodes = Arrays.asList(dbClusterList.split("\\s*,\\s*"));
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
            .mutationTokensEnabled(true)
            .build();
        cluster = CouchbaseCluster.create(env, nodes);
        cluster.authenticate(m_props.getProperty("username"), m_props.getProperty("password"));
        main = cluster.openBucket(m_props.getProperty("main"));
        System.out.println("Connected to couchbase bucket");

        // Load JSON
        List<JsonDocument> documents = new ArrayList<JsonDocument>();
        try (FileReader reader = new FileReader("homes.json")) {
            
            // Read JSON file
            JSONParser jsonParser = new JSONParser();
            Object obj = jsonParser.parse(reader);
            org.json.simple.JSONArray homesList = (org.json.simple.JSONArray) obj;
            for( int i =0; i < homesList.size(); i++ ) {
                documents.add( JsonDocument.create("Doc-"+i, JsonObject.fromJson( homesList.get(i).toString() ) ) );
            }
            

            System.out.println("Loaded JSON into memory");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // Load documents into couchbase
        
        Observable
            .from(documents)
            .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(final JsonDocument docToInsert) {
                    return main.async().insert(docToInsert);
                }
            })
            .last()
            .toBlocking()
            .single();
        System.out.println("All documents have been inserted");

        // Disconnect
        cluster.disconnect();
    }

    public static void LoadProperties(File f) throws IOException {
        FileInputStream propStream = null;
        propStream = new FileInputStream(f);
        m_props.load(propStream);
        propStream.close();
    }
}
