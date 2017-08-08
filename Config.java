package dataparser;

public interface Config {
    public static final String JDBC_RDBMS_URL = "jdbc:oracle:thin:@localhost:1521:orcl";
    
    public static final String KVSTORE_NAME="kvstore";    
    public static final String KVSTORE_URL="localhost:5000";
    
    public static final String KVGRAPH_NAME="my_graph";
    
    public static final String STAGING_PAIRS_PATH="/user/oracle/pgx/stage/tags-pairs-00000";
    public static final String STAGING_AGG_PAIRS_PATH = "/user/oracle/pgx/stageAgg/000000_0";
    
    public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final String HIVE_SERVER_URL = "jdbc:hive2://localhost:10000/default";
}