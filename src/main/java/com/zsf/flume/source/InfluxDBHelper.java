package com.zsf.flume.source;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * InfluxDBHelper
 *
 * @Program: flume-influxdb-source
 * @ClassName: InfluxDBHelper
 * @Author: zhoushengfeng
 * @Create: 2019-10-18 19:34
 * @Email: zhou_shengfeng@163.com
 **/
public class InfluxDBHelper {
    private static final Logger LOG = LoggerFactory
            .getLogger(InfluxDBHelper.class);
    
    private InfluxDBSourceHelper influxDBSourceHelper;
    private InfluxDB influxDB = null;
    
    private String url;
    private String username;
    private String password;
    private String database;
    
    
    public InfluxDBHelper(InfluxDBSourceHelper influxDBSourceHelper) {
        this.influxDBSourceHelper = influxDBSourceHelper;
        /* check for mandatory propertis */
        influxDBSourceHelper.checkMandatoryProperties();
        
        url = influxDBSourceHelper.getConnectionURL();
        username = influxDBSourceHelper.getConnectionUserName();
        password = influxDBSourceHelper.getConnectionPassword();
        database = influxDBSourceHelper.getDatabase();
        
    }
    
    public Object[] executeQuery() {
        Object[] objects = new Object[0];
        QueryResult queryResult;
        if (influxDB == null) {
            establishConnect();
        }
        
        if (influxDBSourceHelper.isCustomQuerySet()) {
            queryResult = influxDB.query(new Query(influxDBSourceHelper.getQuery(), database));
            if (!queryResult.hasError()) {
                //Get the batch data
                List<QueryResult.Result> results = queryResult.getResults();
                if (results.size() > 0) {
                    if (results.get(0).getSeries() != null) {
                        //format :  [value1,value2,...]
                        objects = results.get(0).getSeries().get(0).getValues().toArray();
                    }
                }
            }
        }
        return objects;
    }
    
    
    public void establishConnect() {
        LOG.info("get influxdb connect");
        if (influxDB == null) {
            influxDB = InfluxDBFactory.connect(this.url.startsWith("http://") ? this.url : "http://" + this.url,
                    this.username, this.password);
        }
    }
    
    public void closeConnect() {
        LOG.info("Closing influxDB connect");
        influxDB.close();
    }
}
