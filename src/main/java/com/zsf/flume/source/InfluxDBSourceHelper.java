package com.zsf.flume.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.json.simple.parser.ParseException.ERROR_UNEXPECTED_EXCEPTION;

/**
 * InfluxDBSourceHelper
 *
 * @Program: flume-influxdb-source
 * @ClassName: InfluxDBSource
 * @Author: zhoushengfeng
 * @Create: 2019-10-18 16:42
 * @Email: zhou_shengfeng@163.com
 * <p>
 * <p>
 * Helper to manage configuration parameters and utility methods <p>
 * <p>
 * Configuration parameters readed from flume configuration file:
 * <tt>type: </tt> com.zsf.flume.source.influxDBSource <p>
 * <tt>databases: </tt> table to read from <p>
 * <tt>columns.to.select: </tt> columns to select for import data (* will import all) <p>
 * <tt>run.query.delay: </tt> delay time to execute each query to database <p>
 * <tt>status.file.path: </tt> Directory to save status file <p>
 * <tt>status.file.name: </tt> Name for status file (saves last row index processed) <p>
 * <tt>batch.size: </tt> Batch size to send events from flume source to flume channel <p>
 * <tt>max.rows: </tt> Max rows to import from DB in one query <p>
 * <tt>custom.query: </tt> Custom query to execute to database (be careful) <p>
 **/
public class InfluxDBSourceHelper {
    
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBSourceHelper.class);
    
    private File file, directory;
    private int runQueryDelay, batchSize, maxRows;
    private String startFrom, currentIndex;
    private String statusFilePath, statusFileName, connectionURL, table, database,
            columnsToSelect, customQuery, query, sourceName, delimiterEntry, connectionUserName, connectionPassword,
            defaultCharsetResultSet;
    private Boolean encloseByQuotes;
    
    private Context context;
    private Map<String, String> statusFileJsonMap = new LinkedHashMap<String, String>();
    private boolean readOnlySession;
    
    private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
    private static final int DEFAULT_QUERY_DELAY = 10000;
    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final int DEFAULT_MAX_ROWS = 100000;
    private static final String DEFAULT_INCREMENTAL_VALUE = "1970-01-01T08:00:00.000Z";
    private static final String DEFAULT_DELIMITER_ENTRY = ",";
    private static final Boolean DEFAULT_ENCLOSE_BY_QUOTES = true;
    
    private static final String SOURCE_NAME_STATUS_FILE = "SourceName";
    private static final String URL_STATUS_FILE = "URL";
    private static final String COLUMNS_TO_SELECT_STATUS_FILE = "ColumnsToSelect";
    private static final String TABLE_STATUS_FILE = "Table";
    private static final String LAST_INDEX_STATUS_FILE = "LastTime";
    private static final String QUERY_STATUS_FILE = "Query";
    private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";
    private SimpleDateFormat simpleDateFormat;
    
    private FileReader fileReader;
    private Writer fileWriter;
    
    private JSONParser jsonParser;
    
    /**
     * Builds an InfluxDBSourceHelper containing the configuration parameters and
     *
     * @param context    Flume source context, contains the properties from configuration file
     * @param sourceName source file name for store status
     */
    public InfluxDBSourceHelper(Context context, String sourceName) throws java.text.ParseException {
        
        this.context = context;
        
        statusFilePath = context.getString("status.file.path", DEFAULT_STATUS_DIRECTORY);
        statusFileName = context.getString("status.file.name");
        table = context.getString("table");
        database = context.getString("database");
        columnsToSelect = context.getString("columns.to.select", "*");
        runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
        directory = new File(statusFilePath);
        customQuery = context.getString("custom.query");
        batchSize = context.getInteger("batch.size", DEFAULT_BATCH_SIZE);
        maxRows = context.getInteger("max.rows", DEFAULT_MAX_ROWS);
        connectionURL = context.getString("influxdb.connection.url");
        connectionUserName = context.getString("influxdb.connection.user");
        connectionPassword = context.getString("influxdb.connection.password");
        readOnlySession = context.getBoolean("read.only", false);
        
        this.sourceName = sourceName;
        startFrom = context.getString("start.from", DEFAULT_INCREMENTAL_VALUE);
        delimiterEntry = context.getString("delimiter.entry", DEFAULT_DELIMITER_ENTRY);
        encloseByQuotes = context.getBoolean("enclose.by.quotes", DEFAULT_ENCLOSE_BY_QUOTES);
        statusFileJsonMap = new LinkedHashMap<String, String>();
        defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);
        
        jsonParser = new JSONParser();
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        
        checkMandatoryProperties();
        
        if (!(isStatusDirectoryCreated())) {
            createDirectory();
        }
        
        file = new File(statusFilePath + "/" + statusFileName);
        
        if (!isStatusFileCreated()) {
            currentIndex = startFrom;
            createStatusFile();
        } else {
            currentIndex = getStatusFileIndex(startFrom);
        }
        
        query = buildQuery();
        
    }
    
    public void updateQuery() throws java.text.ParseException {
        this.setCurrentIndex(this.getStatusFileIndex(DEFAULT_INCREMENTAL_VALUE));
        query = buildQuery();
    }
    
    public String buildQuery() throws java.text.ParseException {
        
        if (customQuery == null) {
            return "SELECT " + columnsToSelect + " FROM " + table;
        } else {
            if (customQuery.contains("$@$")) {
                return customQuery.replace("$@$", "'" + currentIndex + "'");
            } else {
                return customQuery;
            }
        }
    }
    
    private boolean isStatusFileCreated() {
        return file.exists() && !file.isDirectory() ? true : false;
    }
    
    private boolean isStatusDirectoryCreated() {
        return directory.exists() && !directory.isFile() ? true : false;
    }
    
    /**
     * Converter from a List of Object List to a List of String arrays <p>
     * Useful for csvWriter
     *
     * @param queryResult Query Result from hibernate executeQuery method
     * @return A list of String arrays, ready for csvWriter.writeall method
     */
    public List<String[]> getAllRows(Object[] queryResult) {
        
        List<String[]> allRows = new ArrayList<String[]>();
        
        if (queryResult.length <= 0) {
            return allRows;
        }
        String[] split;
        String rawRow;
        for (int i = 0; i < queryResult.length; i++) {
            rawRow = queryResult[i].toString();
            if (!StringUtils.isBlank(rawRow)) {
                split = rawRow.substring(1, rawRow.length() - 1).split(", ");
                allRows.add(split);
            }
        }
        return allRows;
    }
    
    /**
     * Create status file
     */
    public void createStatusFile() {
        
        statusFileJsonMap.put(SOURCE_NAME_STATUS_FILE, sourceName);
        statusFileJsonMap.put(URL_STATUS_FILE, connectionURL);
        statusFileJsonMap.put(LAST_INDEX_STATUS_FILE, currentIndex);
        
        if (isCustomQuerySet()) {
            statusFileJsonMap.put(QUERY_STATUS_FILE, customQuery);
        } else {
            statusFileJsonMap.put(COLUMNS_TO_SELECT_STATUS_FILE, columnsToSelect);
            statusFileJsonMap.put(TABLE_STATUS_FILE, table);
        }
        
        try {
            fileWriter = new FileWriter(file, false);
            JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
            fileWriter.close();
        } catch (IOException e) {
            LOG.error("Error creating value to status file!!!", e);
        }
    }
    
    /**
     * Update status file with last read row index
     */
    public void updateStatusFile(String latTime) {
        
        statusFileJsonMap.put(LAST_INDEX_STATUS_FILE, latTime);
        
        try {
            fileWriter = new FileWriter(file, false);
            JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
            fileWriter.close();
        } catch (IOException e) {
            LOG.error("Error writing incremental value to status file!!!", e);
        }
    }
    
    String getStatusFileIndex(String configuredStartValue) {
        
        if (!isStatusFileCreated()) {
            LOG.info("Status file not created, using start value from config file and creating file");
            return configuredStartValue;
        } else {
            try {
                fileReader = new FileReader(file);
                statusFileJsonMap = (Map) jsonParser.parse(fileReader);
                return statusFileJsonMap.get(LAST_INDEX_STATUS_FILE);
                
            } catch (Exception e) {
                LOG.error("Exception reading status file, doing back up and creating new status file", e);
                backupStatusFile();
                return configuredStartValue;
            }
        }
    }
    
    private void checkJsonValues() throws ParseException {
        
        // Check commons values to default and custom query
        if (!statusFileJsonMap.containsKey(SOURCE_NAME_STATUS_FILE) || !statusFileJsonMap.containsKey(URL_STATUS_FILE) ||
                !statusFileJsonMap.containsKey(LAST_INDEX_STATUS_FILE)) {
            LOG.error("Status file doesn't contains all required values");
            throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
        }
        if (!statusFileJsonMap.get(URL_STATUS_FILE).equals(connectionURL)) {
            LOG.error("Connection url in status file doesn't match with configured in properties file");
            throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
        } else if (!statusFileJsonMap.get(SOURCE_NAME_STATUS_FILE).equals(sourceName)) {
            LOG.error("Source name in status file doesn't match with configured in properties file");
            throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
        }
        
        // Check default query values
        if (customQuery == null) {
            if (!statusFileJsonMap.containsKey(COLUMNS_TO_SELECT_STATUS_FILE) || !statusFileJsonMap
                    .containsKey(TABLE_STATUS_FILE)) {
                LOG.error("Expected ColumsToSelect and Table fields in status file");
                throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
            }
            if (!statusFileJsonMap.get(COLUMNS_TO_SELECT_STATUS_FILE).equals(columnsToSelect)) {
                LOG.error("ColumsToSelect value in status file doesn't match with configured in properties file");
                throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
            }
            if (!statusFileJsonMap.get(TABLE_STATUS_FILE).equals(table)) {
                LOG.error("Table value in status file doesn't match with configured in properties file");
                throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
            }
            return;
        }
        
        // Check custom query values
        if (StringUtils.isBlank(customQuery)) {
            if (!statusFileJsonMap.containsKey(QUERY_STATUS_FILE)) {
                LOG.error("Expected Query field in status file");
                throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
            }
            if (!statusFileJsonMap.get(QUERY_STATUS_FILE).equals(customQuery)) {
                LOG.error("Query value in status file doesn't match with configured in properties file");
                throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
            }
            return;
        }
    }
    
    private void backupStatusFile() {
        file.renameTo(new File(statusFilePath + "/" + statusFileName + ".bak." + System.currentTimeMillis()));
    }
    
    public void checkMandatoryProperties() {
        
        /**
         * Necessary attribute detection:  url username password database filepath
         */
        if (connectionURL == null) {
            throw new ConfigurationException("influxdb.connection.url property not set");
        }
        if (statusFileName == null) {
            throw new ConfigurationException("status.file.name property not set");
        }
        
        if (connectionUserName == null) {
            throw new ConfigurationException("influxdb.connection.user property not set");
        }
        
        if (connectionPassword == null) {
            throw new ConfigurationException("influxdb.connection.password property not set");
        }
        if (database == null) {
            throw new ConfigurationException("influxdb.connection.database property not set");
        }
    }
    
    /*
     * @return boolean pathname into directory
     */
    private boolean createDirectory() {
        return directory.mkdir();
    }
    
    /*
     * @return long incremental value as parameter from this
     */
    String getCurrentIndex() {
        return currentIndex;
    }
    
    /*
     * @void set incrementValue
     */
    void setCurrentIndex(String newValue) {
        currentIndex = newValue;
    }
    
    /*
     * @return int delay in ms
     */
    int getRunQueryDelay() {
        return runQueryDelay;
    }
    
    int getBatchSize() {
        return batchSize;
    }
    
    int getMaxRows() {
        return maxRows;
    }
    
    String getQuery() {
        return query;
    }
    
    String getConnectionURL() {
        return connectionURL;
    }
    
    boolean isCustomQuerySet() {
        return (customQuery != null);
    }
    
    Context getContext() {
        return context;
    }
    
    boolean isReadOnlySession() {
        return readOnlySession;
    }
    
    boolean encloseByQuotes() {
        return encloseByQuotes;
    }
    
    String getDelimiterEntry() {
        return delimiterEntry;
    }
    
    public String getConnectionUserName() {
        return connectionUserName;
    }
    
    public String getDatabase() {
        return database;
    }
    
    public String getConnectionPassword() {
        return connectionPassword;
    }
    
    public String getDefaultCharsetResultSet() {
        return defaultCharsetResultSet;
    }
}
