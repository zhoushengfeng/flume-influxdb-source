package com.zsf.flume.source;

import com.opencsv.CSVWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InfluxDB source
 *
 * @Program: flume-influxdb-source
 * @ClassName: InfluxDBSource
 * @Author: 周生锋
 * @Create: 2019-10-18 16:42
 **/
public class InfluxDBSource extends AbstractSource implements Configurable, PollableSource {
    
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBSource.class);
    protected InfluxDBSourceHelper influxDBSourceHelper;
    private CSVWriter csvWriter;
    private InfluxDBHelper influxDBHelper;
    
    
    /**
     * Process a batch of events performing SQL Queries
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        try {
            influxDBSourceHelper.updateQuery();
            Object[] result = influxDBHelper.executeQuery();
            List<String[]> allRows;
            
            if (result.length > 0) {
                allRows = influxDBSourceHelper.getAllRows(result);
                csvWriter.writeAll(allRows, influxDBSourceHelper.encloseByQuotes());
                csvWriter.flush();
                influxDBSourceHelper.updateStatusFile(allRows.get(allRows.size() - 1)[0]);
            }
            
            if (result.length < influxDBSourceHelper.getMaxRows()) {
                Thread.sleep(influxDBSourceHelper.getRunQueryDelay());
            }
            return Status.READY;
        } catch (IOException | InterruptedException | ParseException e) {
            LOG.error("Error procesing row", e);
            return Status.BACKOFF;
        }
    }
    
    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }
    
    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
    
    
    /**
     * Configure the source, load configuration properties and establish connection with influxdb
     */
    @Override
    public void configure(Context context) {
        LOG.getName();
        LOG.info("Reading and processing configuration values for source " + getName());
        
        try {
            influxDBSourceHelper = new InfluxDBSourceHelper(context, getName());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        influxDBHelper = new InfluxDBHelper(influxDBSourceHelper);
        influxDBHelper.establishConnect();
        
        /* Instantiate the CSV Writer */
        csvWriter = new CSVWriter(new ChannelWriter(), influxDBSourceHelper.getDelimiterEntry().charAt(0));
    }
    
    /**
     * Starts the source. Starts the metrics counter.
     */
    @Override
    public void start() {
        
        LOG.info("Starting sql source {} ...", getName());
        super.start();
    }
    
    /**
     * Stop the source. Close database connection and stop metrics counter.
     */
    @Override
    public void stop() {
        
        LOG.info("Stopping sql source {} ...", getName());
        
        try {
            influxDBHelper.closeConnect();
            csvWriter.close();
        } catch (IOException e) {
            LOG.warn("Error CSVWriter object ", e);
        } finally {
            super.stop();
        }
    }
    
    private class ChannelWriter extends Writer {
        private List<Event> events = new ArrayList<>();
        
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            Event event = new SimpleEvent();
            
            String s = new String(cbuf);
            event.setBody(s.substring(off, len - 1).getBytes(Charset.forName(influxDBSourceHelper.getDefaultCharsetResultSet())));
            
            Map<String, String> headers;
            headers = new HashMap<String, String>();
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
            event.setHeaders(headers);
            events.add(event);
            
            if (events.size() >= influxDBSourceHelper.getBatchSize()) {
                flush();
            }
        }
        
        @Override
        public void flush() throws IOException {
            getChannelProcessor().processEventBatch(events);
            events.clear();
        }
        
        @Override
        public void close() throws IOException {
            flush();
        }
    }
    
}
