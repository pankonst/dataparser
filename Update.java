package dataparser;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

import java.io.InputStreamReader;

import java.sql.Connection;

import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;

import java.util.List;
import java.util.Map;

import java.util.function.Function;
import java.util.stream.Collectors;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import oracle.kv.StatementResult;
import oracle.kv.table.TableAPI;

import oracle.pg.nosql.OraclePropertyGraph;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Update {
    private OraclePropertyGraph opg;
    private String[] hhosts = { Config.KVSTORE_URL };
    private KVStoreConfig kvconfig;
    private KVStore kvstore;
    private Connect connect;
    private TableAPI tableApi;
    private FileSystem fs;
    private List<String> tagList = new ArrayList<String>();
    private List<String> tagPairs = new ArrayList<String>();
    
    public Update() {
        super();
        this.connect = new Connect();
        this.kvconfig = connect.getNoSQLConfig(hhosts, Config.KVSTORE_NAME);
        this.kvstore = KVStoreFactory.getStore(kvconfig);
        this.tableApi = kvstore.getTableAPI();
        
        System.out.println("Retreiving NoSQL Property Graph...");
        
        try{
            opg = OraclePropertyGraph.getInstance(kvconfig, Config.KVGRAPH_NAME);
        } catch(Exception e){
            System.out.println(e.getMessage());
        }
        
        System.out.println("NoSQL Property Graph retreived...\n");
    }
    
    public void updateOracleNoSQLGraph(String hdfsPath){
        String fileContent = null;
        InputStream fileContentStream = null;
        String[] tagsArray;
        
        System.out.println("Connecting to HDFS...");
        
        Connect connectHdfs = new Connect();
        fs = connectHdfs.connectHadoop();
        
        System.out.println("Connection to HDFS established...\n");
        
        Path path = new Path(hdfsPath);
        
        try{
            fileContent = IOUtils.toString(fs.open(path), "UTF-8");
            
            fileContentStream = IOUtils.toInputStream(fileContent, "UTF-8");            
            BufferedReader br = new BufferedReader(new InputStreamReader(fileContentStream));
            String line = br.readLine();
            
            // Recursive function to create the tag and tag pair lists
            while(line!=null){
                tagsArray = line.split("\\s");
                createTagsAndPairsList(tagsArray[0], Arrays.copyOfRange(tagsArray, tagsArray.length-(tagsArray.length-1), tagsArray.length));
                line = br.readLine();
            }
            
            // Get occurences of each tag in tagList
            Map<String, Long> tagsMap = tagList.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            
            // Insert new tag/vertex or update existing one
            try{
                updateVertices(tagsMap);
            } catch(SQLException e){
                System.out.println(e.getMessage());
            }
            
            System.out.println("Vertices updated...");
            
            // Get occurences of each pair in edgeMap
            Map<String, Long> edgeMap = tagPairs.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            
            // Insert new edge or update existing one
            updateEdges(edgeMap);
            
            System.out.println("Edges updated...");
            
            br.close();
            
            opg.commit();
            opg.closeKVStore();
        } catch(IOException e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void updateVertices(Map<String, Long> tagsMap) throws SQLException{
        Vertex v;
        String query;
        
        System.out.println("Connecting to Oracle Database...");
        Connection dbConn = connect.connectRDB(Config.JDBC_RDBMS_URL, "pgx", "pgx");
        System.out.println("Connection to Database established...");
        
        Statement state = dbConn.createStatement();
        
        for(Entry<String, Long> tag : tagsMap.entrySet()){
            v = opg.getVertex(tag.getKey());
            
            if(v!=null){
                long updCount = Long.parseLong(v.getProperty("count").toString()) + tag.getValue();
                v.setProperty("count", updCount);
                
                query = "UPDATE PGX.TAGS SET COUNT=" + updCount + " WHERE TAGNAME='" + tag.getKey() + "'";
                state.execute(query);
            } else {
                v = opg.addVertex(tag.getKey());
                v.setProperty("tagName", tag.getKey());
                v.setProperty("count", tag.getValue());
                
                query = "INSERT INTO PGX.TAGS (TAGNAME, COUNT) VALUES ('" + tag.getKey() + "', " + tag.getValue() + ")";
                state.execute(query);
            }
        }
        
        dbConn.commit();
        dbConn.close();
    }
    
    private void updateEdges(Map<String, Long> edgeMap){
        Vertex v1;
        Vertex v2;
        Edge e;
        
        for(Entry<String, Long> edge : edgeMap.entrySet()){
            String[] tags = edge.getKey().split("\\s");
            
            v1 = opg.getVertex(tags[0]);
            v2 = opg.getVertex(tags[1]);
            
            if(v1==null || v2 == null){
                continue;
            }
            
            // Check if the edge exists in the NoSQL edge table
            StatementResult sr = kvstore.executeSync("select eid from my_graphGE_ where svid=" + v1.getId() + " and dvid=" + v2.getId());
            
            // If it does update the weight property. If not, add it and set the weight property to 1
            if(sr.iterator().hasNext()){
                String id = sr.iterator().next().get("eid").toString();
                e = opg.getEdge(id);
                
                Long updWeight = Long.parseLong(e.getProperty("weight").toString()) + edge.getValue();
                e.setProperty("weight", updWeight);
            } else{
                e = opg.addEdge(edge.getKey(), v1, v2, "connects");
                e.setProperty("weight", 1);
            }
        }
    }
    
    private void createTagsAndPairsList(String first, String[] rest){
        if(rest.length == 0){
            tagList.add(first);
            return;
        }
        
        tagList.add(first);
        
        for(String tag : rest){
            tagPairs.add(first + " " + tag);
        }
        
        createTagsAndPairsList(rest[0], Arrays.copyOfRange(rest, rest.length-(rest.length-1), rest.length));
        return;
    }
}
