## reference configuration      * represents required

    # Describe/configure the source  
    a1.sources.r1.type = com.zsf.flume.source.InfluxDBSource
    
    # source type 
    # url = ip:port  *
    a1.sources.r1.influxdb.connection.url = 192.168.254.128:8086
    
    #Collected data source info: username password * 
    a1.sources.r1.influxdb.connection.user = root
    a1.sources.r1.influxdb.connection.password = root
    a1.sources.r1.influxdb.connection.autocommit = true
    
    # database name * 
    a1.sources.r1.database = zhousf
    
    # Query delay, each configured milisecond the query will be sent
    # interval time
    a1.sources.r1.run.query.delay=10000
    
    # Status file is used to save last readed row
    # Incrementally update the condition data savepoint * 
    a1.sources.r1.status.file.path = /export/data/sqlSource
    a1.sources.r1.status.file.name = sqlSource.status
    
    # Custom query statement
    #a1.sources.r1.start.from = -1
    #Start the query from the record whose time is 1970-01-01 t08:00:00.000Z, and the record is identified as time * 
    a1.sources.r1.custom.query = select * from test where time > $@$
    
    a1.sources.r1.batch.size = 5000
    a1.sources.r1.max.rows = 10000
   
Some of the code borrows from flume-ng-sql,Thanks a lot.
