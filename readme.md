## reference configuration      * represents required

    # Describe/configure the source  
    a1.sources.r1.type = com.zsf.flume.source.InfluxDBSource
    
    #source type 声明  url = ip:port  *
    a1.sources.r1.influxdb.connection.url = 192.168.254.128:8086
    
    #被采集数据源 username password * 
    a1.sources.r1.influxdb.connection.user = root
    a1.sources.r1.influxdb.connection.password = root
    a1.sources.r1.influxdb.connection.autocommit = true
    
    # database name * 
    a1.sources.r1.database = zhousf
    
    # Query delay, each configured milisecond the query will be sent
    # 批次时间
    a1.sources.r1.run.query.delay=10000
    
    # Status file is used to save last readed row
    # 增量更新条件数据保存点 * 
    a1.sources.r1.status.file.path = /export/data/sqlSource
    a1.sources.r1.status.file.name = sqlSource.status
    
    # 自定义查询语句 
    #a1.sources.r1.start.from = -1
    #从time为 1970-01-01T08:00:00.000Z 的记录开始查询，记录标识为time * 
    a1.sources.r1.custom.query = select * from test where time > $@$
    
    a1.sources.r1.batch.size = 5000
    a1.sources.r1.max.rows = 10000
   
