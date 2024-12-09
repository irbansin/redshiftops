package in.anirbansinha.redshift;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.github.cdimascio.dotenv.Dotenv;

import java.io.File;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;

/**
 * Performs SQL DDL and SELECT queries on a MySQL database hosted on AWS RDS.
 */
public class AmazonRedshift {
    /**
     * Connection to database
     */
    private Connection con;

    /**
     * TODO: Fill in AWS connection information.
     */
    // private String url = ;
    // private String uid = ;
    // private String pw = ;
    /**
     * Main method is only used for convenience. Use JUnit test file to verify your
     * answer.
     *
     * @param args
     *             none expected
     * @throws SQLException
     *                      if a database error occurs
     */
    public static void main(String[] args) throws SQLException {
        AmazonRedshift q = new AmazonRedshift();
        q.connect();
        // q.drop();
        // q.create();
        // q.insert();
        System.out.println(resultSetToString(q.query1(), 30));
        // q.query2();
        // q.query3();
        q.close();
    }

    /**
     * Makes a connection to the database and returns connection to caller.
     *
     * @return
     *         connection
     * @throws SQLException
     *                      if an error occurs
     */
    public Connection connect() throws SQLException {
        Dotenv dotenv = Dotenv.load();

        // AWS Redshift connection details
        String url = dotenv.get("DB_URL");
        String uid = dotenv.get("DB_USERNAME");
        String pw = dotenv.get("DB_PASSWORD");
    
        System.out.println("Connecting to database...");
        try {
            con = DriverManager.getConnection(url, uid, pw);
            System.out.println("Connected successfully.");
        } catch (SQLException e) {
            System.err.println("Connection failed: " + e.getMessage());
            throw e; 
        }
        return con;
    }
    

    /**
     * Closes connection to database.
     */
    public void close() {
        System.out.println("Closing database connection...");
        if (con != null) {
            try {
                con.close(); 
                System.out.println("Connection closed successfully.");
            } catch (SQLException e) {
                System.err.println("Failed to close the connection: " + e.getMessage());
            }
        } else {
            System.out.println("No connection to close.");
        }
    }
    

    public void drop() {
        System.out.println("Dropping all the tables");
    
        // SQL to drop tables in the dev schema
        String dropTablesSQL =
            "DROP TABLE IF EXISTS dev.PART CASCADE;\n" +
            "DROP TABLE IF EXISTS dev.SUPPLIER CASCADE;\n" +
            "DROP TABLE IF EXISTS dev.PARTSUPP CASCADE;\n" +
            "DROP TABLE IF EXISTS dev.CUSTOMER CASCADE;\n" +
            "DROP TABLE IF EXISTS dev.ORDERS CASCADE;\n" +
            "DROP TABLE IF EXISTS dev.LINEITEM CASCADE;\n" +
            "DROP TABLE IF EXISTS dev.NATION CASCADE;\n" +
            "DROP TABLE IF EXISTS dev.REGION CASCADE;";
    
        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(dropTablesSQL);
            System.out.println("Tables dropped successfully.");
        } catch (SQLException e) {
            System.err.println("Error while dropping tables: " + e.getMessage());
        }
    }
    
    
    public void create() throws SQLException {
        System.out.println("Creating Tables");
    
        // Define the DDL queries for creating tables
        String createTablesSQL = 
            "DROP TABLE IF EXISTS dev.part CASCADE;\n" +
            "CREATE TABLE dev.part (\n" +
            "    P_PARTKEY      INTEGER PRIMARY KEY,\n" +
            "    P_NAME         VARCHAR(55),\n" +
            "    P_MFGR         CHAR(25),\n" +
            "    P_BRAND        CHAR(10),\n" +
            "    P_TYPE         VARCHAR(25),\n" +
            "    P_SIZE         INTEGER,\n" +
            "    P_CONTAINER    CHAR(10),\n" +
            "    P_RETAILPRICE  DECIMAL,\n" +
            "    P_COMMENT      VARCHAR(23)\n" +
            ");\n" +
    
            "DROP TABLE IF EXISTS dev.supplier CASCADE;\n" +
            "CREATE TABLE dev.supplier (\n" +
            "    S_SUPPKEY      INTEGER PRIMARY KEY,\n" +
            "    S_NAME         CHAR(25),\n" +
            "    S_ADDRESS      VARCHAR(40),\n" +
            "    S_NATIONKEY    BIGINT NOT NULL,\n" +
            "    S_PHONE        CHAR(15),\n" +
            "    S_ACCTBAL      DECIMAL,\n" +
            "    S_COMMENT      VARCHAR(101)\n" +
            ");\n" +
    
            "DROP TABLE IF EXISTS dev.partsupp CASCADE;\n" +
            "CREATE TABLE dev.partsupp (\n" +
            "    PS_PARTKEY     BIGINT NOT NULL,\n" +
            "    PS_SUPPKEY     BIGINT NOT NULL,\n" +
            "    PS_AVAILQTY    INTEGER,\n" +
            "    PS_SUPPLYCOST  DECIMAL,\n" +
            "    PS_COMMENT     VARCHAR(199),\n" +
            "    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)\n" +
            ");\n" +
    
            "DROP TABLE IF EXISTS dev.customer CASCADE;\n" +
            "CREATE TABLE dev.customer (\n" +
            "    C_CUSTKEY      INTEGER PRIMARY KEY,\n" +
            "    C_NAME         VARCHAR(25),\n" +
            "    C_ADDRESS      VARCHAR(40),\n" +
            "    C_NATIONKEY    BIGINT NOT NULL,\n" +
            "    C_PHONE        CHAR(15),\n" +
            "    C_ACCTBAL      DECIMAL,\n" +
            "    C_MKTSEGMENT   CHAR(10),\n" +
            "    C_COMMENT      VARCHAR(117)\n" +
            ");\n" +
    
            "DROP TABLE IF EXISTS dev.orders CASCADE;\n" +
            "CREATE TABLE dev.orders (\n" +
            "    O_ORDERKEY     INTEGER PRIMARY KEY,\n" +
            "    O_CUSTKEY      BIGINT NOT NULL,\n" +
            "    O_ORDERSTATUS  CHAR(1),\n" +
            "    O_TOTALPRICE   DECIMAL,\n" +
            "    O_ORDERDATE    DATE,\n" +
            "    O_ORDERPRIORITY CHAR(15),\n" +
            "    O_CLERK        CHAR(15),\n" +
            "    O_SHIPPRIORITY INTEGER,\n" +
            "    O_COMMENT       VARCHAR(79)\n" +
            ");\n" +
    
            "DROP TABLE IF EXISTS dev.lineitem CASCADE;\n" +
            "CREATE TABLE dev.lineitem (\n" +
            "    L_ORDERKEY     BIGINT NOT NULL,\n" +
            "    L_PARTKEY      BIGINT NOT NULL,\n" +
            "    L_SUPPKEY      BIGINT NOT NULL,\n" +
            "    L_LINENUMBER   INTEGER,\n" +
            "    L_QUANTITY     DECIMAL,\n" +
            "    L_EXTENDEDPRICE DECIMAL,\n" +
            "    L_DISCOUNT     DECIMAL,\n" +
            "    L_TAX          DECIMAL,\n" +
            "    L_RETURNFLAG   CHAR(1),\n" +
            "    L_LINESTATUS   CHAR(1),\n" +
            "    L_SHIPDATE     DATE,\n" +
            "    L_COMMITDATE   DATE,\n" +
            "    L_RECEIPTDATE  DATE,\n" +
            "    L_SHIPINSTRUCT CHAR(25),\n" +
            "    L_SHIPMODE     CHAR(10),\n" +
            "    L_COMMENT      VARCHAR(44),\n" +
            "    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)\n" +
            ");\n" +
    
            "DROP TABLE IF EXISTS dev.nation CASCADE;\n" +
            "CREATE TABLE dev.nation (\n" +
            "    N_NATIONKEY    INTEGER PRIMARY KEY,\n" +
            "    N_NAME         CHAR(25),\n" +
            "    N_REGIONKEY    BIGINT NOT NULL,\n" +
            "    N_COMMENT      VARCHAR(152)\n" +
            ");\n" +
    
            "DROP TABLE IF EXISTS dev.region CASCADE;\n" +
            "CREATE TABLE dev.region (\n" +
            "    R_REGIONKEY    INTEGER PRIMARY KEY,\n" +
            "    R_NAME         CHAR(25),\n" +
            "    R_COMMENT      VARCHAR(152)\n" +
            ");";
    
        // Execute the DDL statements
        Statement stmt = con.createStatement();
        stmt.executeUpdate(createTablesSQL);
        
        System.out.println("Tables created successfully.");
    }
    
    public void insert() throws SQLException {
        System.out.println("Loading TPC-H Data");
    
        // Paths to your DDL files (adjust as per your directory structure)
        String[] ddlFiles = {
            "ddl/part.sql",
            "ddl/supplier.sql",
            "ddl/partsupp.sql",
            "ddl/customer.sql",
            "ddl/orders.sql",
            "ddl/lineitem.sql",
            "ddl/nation.sql",
            "ddl/region.sql"
        };
        
        String regex = "(?i)(INSERT INTO\\s+)(\\w+)";
        Pattern pattern = Pattern.compile(regex);
        // Loop through each file and execute its content
        for (String file : ddlFiles) {
            try {
                // Read the file content as a single string
                String ddlContent = new String(Files.readAllBytes(Paths.get(file)));
                System.out.println("Reading "+ file);
                // Split the content into batches if the file is too large
                String[] statements = ddlContent.split(";");
    
                try (Statement stmt = con.createStatement()) {
                    for (String statement : statements) {
                        if (!statement.trim().isEmpty()) {
                            Matcher matcher = pattern.matcher(statement);
                            String modifiedQuery = matcher.replaceFirst("$1dev.$2");

                            stmt.executeUpdate(modifiedQuery.trim());
                        }
                    }
                    System.out.println("Data loaded from " + file);
                } catch (SQLException e) {
                    System.err.println("Error loading data from " + file + ": " + e.getMessage());
                }
            } catch (IOException e) {
                System.err.println("Error reading file " + file + ": " + e.getMessage());
            }
        }
    }
    

    /**
     * Query returns the most recent top 10 orders with the total sale and the date
     * of the
     * order in `America`.
     *
     * @return
     *         ResultSet
     * @throws SQLException
     *                      if an error occurs
     */
    public ResultSet query1() throws SQLException {
        System.out.println("Executing query #1.");
        
        // SQL Query to get the most recent top 10 orders with total sale and order date for customers in America
        String query1SQL = 
            "SELECT O.O_ORDERKEY, O.O_TOTALPRICE, O.O_ORDERDATE " +
            "FROM dev.orders O " +
            "JOIN dev.customer C ON O.O_CUSTKEY = C.C_CUSTKEY " +
            "JOIN dev.nation N ON C.C_NATIONKEY = N.N_NATIONKEY " +
            "WHERE N.N_NAME = 'UNITED STATES' " +
            "ORDER BY O.O_ORDERDATE DESC " +
            "LIMIT 10;";
        
        // Execute query
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query1SQL);
    }
    
    

    /**
     * Query returns the customer key and the total price a customer spent in
     * descending
     * order, for all urgent orders that are not failed for all customers who are
     * outside Europe belonging
     * to the highest market segment.
     *
     * @return
     *         ResultSet
     * @throws SQLException
     *                      if an error occurs
     */
    public ResultSet query2() throws SQLException {
        System.out.println("Executing query #2.");
    
        // SQL Query to get the customer key and the total price spent by customers
        // who are outside Europe and belong to the largest market segment
        String query2SQL =
            "WITH LargestMarketSegment AS (" +
            "    SELECT C_MKTSEGMENT, COUNT(*) AS customer_count " +
            "    FROM dev.customer " +
            "    WHERE C_MKTSEGMENT IS NOT NULL " +
            "    GROUP BY C_MKTSEGMENT " +
            "    ORDER BY customer_count DESC " +
            "    LIMIT 1" +
            ") " +
            "SELECT C.C_CUSTKEY, SUM(O.O_TOTALPRICE) AS total_spent " +
            "FROM dev.customer C " +
            "JOIN dev.orders O ON C.C_CUSTKEY = O.O_CUSTKEY " +
            "JOIN dev.nation N ON C.C_NATIONKEY = N.N_NATIONKEY " +
            "JOIN LargestMarketSegment L ON C.C_MKTSEGMENT = L.C_MKTSEGMENT " +
            "WHERE N.N_NAME != 'Europe' AND O.O_ORDERSTATUS = 'U' " +
            "GROUP BY C.C_CUSTKEY " +
            "ORDER BY total_spent DESC;";
        
        // Execute query
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query2SQL);
    }
    
    

    /**
     * Query returns all the lineitems that was ordered within the six years from
     * January 4th,
     * 1997 and the orderpriority in ascending order.
     *
     * @return
     *         ResultSet
     * @throws SQLException
     *                      if an error occurs
     */
    public ResultSet query3() throws SQLException {
        System.out.println("Executing query #3.");
    
        // SQL Query to count the line items ordered within six years from April 1st, 1997
        // and group them by order priority, sorted by order priority
        String query3SQL = 
            "SELECT O.O_ORDERPRIORITY, COUNT(L.L_ORDERKEY) AS lineitem_count " +
            "FROM dev.lineitem L " +
            "JOIN dev.orders O ON L.L_ORDERKEY = O.O_ORDERKEY " +
            "WHERE L.L_SHIPDATE BETWEEN '1997-04-01' AND '2003-04-01' " +
            "GROUP BY O.O_ORDERPRIORITY " +
            "ORDER BY O.O_ORDERPRIORITY ASC;";
        
        // Execute query
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query3SQL);
    }
    
    /*
     * Do not change anything below here.
     */
    /**
     * Converts a ResultSet to a string with a given number of rows displayed.
     * Total rows are determined but only the first few are put into a string.
     *
     * @param rst
     *                ResultSet
     * @param maxrows
     *                maximum number of rows to display
     * @return
     *         String form of results
     * @throws SQLException
     *                      if a database error occurs
     */
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }

    /**
     * Converts ResultSetMetaData into a string.
     *
     * @param meta
     *             ResultSetMetaData
     * @return
     *         string form of metadata
     * @throws SQLException
     *                      if a database error occurs
     */
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        buf.append(meta.getColumnName(1) + " (" + meta.getColumnLabel(1) + "," + meta.getColumnType(1) + "-"
                + meta.getColumnTypeName(1) + "," + meta.getColumnDisplaySize(1) + ", " + meta.getPrecision(1) + ", "
                + meta.getScale(1) + ")");
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j) + " (" + meta.getColumnLabel(j) + "," + meta.getColumnType(j) + "-"
                    + meta.getColumnTypeName(j) + "," + meta.getColumnDisplaySize(j) + ", " + meta.getPrecision(j)
                    + ", " + meta.getScale(j) + ")");
        return buf.toString();
    }
}