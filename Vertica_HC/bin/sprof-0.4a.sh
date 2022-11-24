#!/bin/bash

#--------------------------------------------------------------------------
# sprof version 0.4a - September 2019
# vim: et:ts=4:sw=4:sm:ai:
#--------------------------------------------------------------------------

#---------------------------------------------------------------------------
# Setting Default Values
#---------------------------------------------------------------------------
SPV="sprof 0.4a"
OUT=sprof.out
GZP=0
VMON=v_monitor  # Vertica monitor  schema
VINT=v_internal # Vertica internal schema
VCAT=v_catalog  # Vertica catalog schema
SDATE=0001-01-01
EDATE=9999-12-31

usage="Usage: sprof [-o | --output out_file] [-g |--gzip] [-c schema] [-m schema] [-i schema] [-S start] [-E end] [-h|--help]\n"
usage+="    -o | --output out_file: defines output file (default sprof.out)\n"
usage+="    -g | --gzip: gzips output file)\n"
usage+="    -m schema: defines monitoring schema (default ${VMON})\n"
usage+="    -i schema: defines dc_tables schema (default ${VINT})\n"
usage+="    -c schema: defines catalog schema (default ${VCAT})\n"
usage+="    -S YYYY-MM-DD: defines start date (default ${SDATE})\n"
usage+="    -E YYYY-MM-DD: defines end date (default ${EDATE})\n"
usage+="    -h | --help: prints this message"

#---------------------------------------------------------------------------
# Command line options handling
#---------------------------------------------------------------------------
while [ $# -gt 0 ]; do
    case "$1" in
        "--output" | "-o")
            OUT=$2
            shift 2
           ;;
        "--gzip" | "-g")
            GZP=1
            shift
            ;;
        "-m")
            VMON=$2
            shift 2
            ;;
        "-i")
            VINT=$2
            shift 2
            ;;
        "-c")
            VCAT=$2
            shift 2
            ;;
        "-S")
            SDATE=$2
            shift 2
            ;;
        "-E")
            EDATE=$2
            shift 2
            ;;
        "--help" | "-h")
            echo -e $usage
            exit 0
            ;;
        *)
            echo "[sprof] WARNING: invalid option '$1'. Ignored"
            shift 1
            ;;
    esac
done

#---------------------------------------------------------------------------
# Check user has dbadmin role
#---------------------------------------------------------------------------
if [ $(vsql -XAtqn -c "SELECT HAS_ROLE('dbadmin')") == 'f' ] ; then
    echo "User has no dbadmin role"
    exit 1
fi
#------------------------------------------------------------------------
# Start of sprof
#------------------------------------------------------------------------
secs=`date +%s`
echo "[`date +'%Y-%m-%d %H:%M:%S'`] ${SPV} started"
echo "Dates range: ${SDATE} to ${EDATE}. Output to ${OUT}"
 
#---------------------------------------------------------------------------
# Running system profile analysis
#---------------------------------------------------------------------------
cat <<-EOF | vsql -Xqn -P null='NULL' -o ${OUT} -f -
    \qecho ###------------------------------------------
    \qecho ### ${SPV}
    \qecho ### catalog schema: ${VCAT}
    \qecho ### monitor schema: ${VMON}
    \qecho ### internal schema: ${VINT}
    \qecho ### output file: ${OUT}
    \qecho ### gzip flag: ${GZP}
    \qecho ### start date: ${SDATE}
    \qecho ### end date: ${EDATE}
    \qecho ###------------------------------------------
    \qecho

    \set sdate '''${SDATE}''::TIMESTAMP'
    \set edate '''${EDATE}''::TIMESTAMP'

    -- ------------------------------------------------------------------------
    -- Start
    -- ------------------------------------------------------------------------
    \echo '    Step 0a: Script start timestamp'
    \qecho >>> Step 0a: Script start timestamp
    SELECT SYSDATE() AS 'Start Timestamp' ;

    \echo '    Step 0b: Flushing Data Collector'
    \qecho >>> Step 0b: Flushing Data Collector
    SELECT FLUSH_DATA_COLLECTOR(); 

    -- ------------------------------------------------------------------------
    -- System Information
    -- ------------------------------------------------------------------------
    \echo '    Step 1a: Vertica version'
    \qecho >>> Step 1a: Vertica version
    SELECT
        VERSION() 
    ;

    \pset expanded
    \echo '    Step 1b: System Information'
    \qecho >>> Step 1b: System Information
    SELECT * FROM ${VMON}.system ;
    \pset expanded

    -- ------------------------------------------------------------------------
    -- Get Data Collector Policy
    -- ------------------------------------------------------------------------
    \echo '    Step 2: Data Collector Policy'
    \qecho >>> Step 2: Data Collector Policy
    SELECT
        GET_DATA_COLLECTOR_POLICY('RequestsIssued');
    ;

    -- ------------------------------------------------------------------------
    -- Getting Vertica non-default configuration parameters
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 3: Vertica non-default configuration parameters'
    \qecho >>> Step 3: Vertica non-default configuration parameters
    SELECT
        parameter_name, 
        current_value, 
        default_value, 
        description
    FROM 
        configuration_parameters 
    WHERE
        current_value <> default_value
    ORDER BY 
        parameter_name;
    ;
    \pset expanded

    -- ------------------------------------------------------------------------
    -- Resource Pools configuration
    -- ------------------------------------------------------------------------
    \echo '    Step 4: Resource Pools configuration'
    \qecho >>> Step 4: Resource Pools configuration
    SELECT
        rp.name,
        rp.memorysize AS MS,
        rp.maxmemorysize AS MMS,
        rp.maxquerymemorysize AS MQMS,
        rp.executionparallelism AS EP,
        rp.plannedconcurrency AS PC,
        rp.maxconcurrency as MC,
        rp.queuetimeout AS QT,
        rp.priority AS PRI,
        rp.runtimepriority AS RTPRI,
        rp.runtimeprioritythreshold AS RTPSH,
        rp.singleinitiator AS SI,
        rp.cpuaffinityset AS CPUA,
        rp.cpuaffinitymode AS CPUM,
        rp.runtimecap AS RTCAP,
        rp.cascadeto AS CASCADE,
        rs.query_budget_kb//1024 AS QBMB
    FROM
        ${VCAT}.resource_pools rp
        INNER JOIN ${VMON}.resource_pool_status rs ON rp.name = rs.pool_name
    ORDER BY
        rp.is_internal, rp.name
    ;

    -- ------------------------------------------------------------------------
    -- Cluster Analysis
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 5a: Getting Cluster configuration'
    \qecho >>> Step 5a: Getting Cluster configuration
    SELECT
        *
    FROM
        ${VMON}.host_resources
    ;
    \pset expanded
    \echo '    Step 5b: Getting Elastic Cluster configuration'
    \qecho >>> Step 5b: Getting Elastic Cluster configuration
    SELECT
        version, 
        skew_percent, 
        scaling_factor, 
        is_enabled, 
        is_local_segment_enabled
    FROM 
        ${VINT}.vs_elastic_cluster
    ;

    \echo '    Step 5c: Spread Retransmit'
    \qecho >>> Step 5c: Spread Retransmit

    SELECT
        a."time" ,
        a.node_name ,
        a.retrans ,
        a.time_interval ,
        a.packet_count ,
        ((a.retrans / (a.time_interval / '00:00:01'::INTERVAL)))::NUMERIC(18,2) AS retrans_per_second
    FROM (
        SELECT
            (dc_spread_monitor."time")::timestamp AS "time" ,
            dc_spread_monitor.node_name ,
            (dc_spread_monitor.retrans -
             lag(dc_spread_monitor.retrans, 1, NULL::INT) OVER(
                PARTITION BY dc_spread_monitor.node_name 
                ORDER BY (dc_spread_monitor."time")::TIMESTAMP)) AS retrans,
            (((dc_spread_monitor."time")::TIMESTAMP -
               lag((dc_spread_monitor."time")::TIMESTAMP, 1, NULL::TIMESTAMP) OVER(
                PARTITION BY dc_spread_monitor.node_name
                ORDER BY (dc_spread_monitor."time")::TIMESTAMP))) AS time_interval,
            (dc_spread_monitor.packet_sent -
             lag(dc_spread_monitor.packet_sent, 1, NULL::INT) OVER(
                PARTITION BY dc_spread_monitor.node_name
                ORDER BY (dc_spread_monitor."time")::TIMESTAMP)) AS packet_count
        FROM
            ${VINT}.dc_spread_monitor
    ) a
    WHERE a.time BETWEEN :sdate AND :edate
    ORDER BY 1, 2
    ;
           
    \echo '    Step 5d: Things slower than expected'
    \qecho >>> Step 5d: Things slower than expected
    SELECT
        event_description ,
        count(*) ,
        MAX(threshold_us/1000)::INT As Max_threshold_ms ,
        MAX(duration_us /1000)::INT As Max_duration_ms
    FROM
        ${VINT}.dc_slow_events
    GROUP BY 1
    ORDER BY COUNT(*) DESC LIMIT 5
    ;

    -- ------------------------------------------------------------------------
    -- Database Size (raw & compressed)
    -- ------------------------------------------------------------------------
    \echo '    Step 6a: Database Size (raw)'
    \qecho >>> Step 6a: Database Size (raw)
    SELECT
        GET_COMPLIANCE_STATUS()
    ;

    \echo '    Step 6b: Database Size (compressed by schema)'
    \qecho >>> Step 6b: Database Size (compressed by schema)
    SELECT
        anchor_table_schema,
        COUNT(DISTINCT(anchor_table_name)) AS num_tables,
        COUNT(DISTINCT(projection_name)) AS num_projs,
        COUNT(*) AS num_psegs,
        (SUM(used_bytes)/(1024^3))::INT AS used_gib 
    FROM
        ${VMON}.projection_storage
    GROUP BY ROLLUP(anchor_table_schema)
    ORDER BY 
        4 desc
    ;

 -- MF: Do we really need this?     
 -- \echo '    Step 6c: License Usage to determine compression'
 -- \qecho >>> Step 6c: License Usage to determine compression

 -- SELECT
 --     date_trunc('hour',audit_start_timestamp) audit_time ,
 --     database_size_bytes ,
 --     usage_percent ,
 --     audited_data ,
 --     license_size_bytes
 -- FROM
 --     ${VCAT}.license_audits
 -- WHERE
 --     database_size_bytes > 0
 -- ORDER BY
 --     audit_start_timestamp desc
 -- ;

    -- ------------------------------------------------------------------------
    -- Catalog analysis (7a-7e from Eugenia's analyze_catalog)
    -- ------------------------------------------------------------------------
    \echo '    Step 7a: Catalog Analysis (Column types)'
    \qecho >>> Step 7a: Catalog Analysis (Column types)
    SELECT
        data_type_id, 
        MAX(UPPER(split_part(data_type,'(',1))) AS  data_type, 
        COUNT(*) AS num_colums, 
        COUNT(DISTINCT table_id) AS num_tables,  
        MAX(data_type_length) AS max_length, 
        AVG(data_type_length)::INT AS avg_length  
    FROM 
        ${VCAT}.columns GROUP BY 1 ORDER BY 1;

    \echo '    Step 7b: Catalog Analysis (Top 30 Largest Schemas)'
    \qecho >>> Step 7b: Catalog Analysis (Top 30 Largest Schemas)
    SELECT
        anchor_table_schema, 
        COUNT(DISTINCT(anchor_table_name)) AS table_count,
        SUM(used_bytes)//1024^3 AS gb_size, 
        SUM(ros_count)//1000 AS krows 
    FROM 
        ${VMON}.projection_storage 
    GROUP BY 1
    ORDER BY 2 desc
    LIMIT 30;

    \echo '    Step 7c: Catalog Analysis (Top 30 Tables with more columns)'
    \qecho >>> Step 7c: Catalog Analysis (Top 30 Tables with more columns)
    SELECT
        table_schema, 
        table_name, 
        COUNT(DISTINCT column_id) AS '#cols', 
        SUM(data_type_length) AS sum_col_length
    FROM 
        ${VCAT}.columns 
    GROUP BY 1,2 
    ORDER BY 3 desc 
    LIMIT 30;

    \echo '    Step 7d: Catalog Analysis (Top 30 Tables with largest rows)'
    \qecho >>> Step 7d: Catalog Analysis (Top 30 Tables with largest rows)
    SELECT
        table_schema, 
        table_name, 
        COUNT(DISTINCT column_id) AS '#cols', 
        SUM(data_type_length) AS sum_col_length
    FROM 
        ${VCAT}.columns 
    GROUP BY 1,2 
    ORDER BY 4 desc 
    LIMIT 30;

    \echo '    Step 7e: Catalog Analysis (Top 30 largest segmented projections)'
    \qecho >>> Step 7e: Catalog Analysis (Top 30 largest segmented projections)
        SELECT
        ps.anchor_table_schema || '.' ||  ps.anchor_table_name ||
            '.' || ps.projection_name AS 'schema.table.projection' ,
        SUM(ps.row_count) AS row_count, 
        SUM(ps.used_bytes)//1024^2 AS used_mbytes,
        (CASE WHEN MIN(ps.row_count) = 0 THEN -1000.00
              ELSE MAX(ps.row_count)/MIN(ps.row_count) END)::NUMERIC(8,2) AS skew_ratio,
        SUM(ps.wos_row_count) AS wos_rows, 
        SUM(ps.wos_used_bytes)//1024^2 AS wos_mbytes, 
        SUM(ps.ros_row_count) AS ros_rows, 
        SUM(ps.ros_used_bytes)//1024^2 AS ros_mbytes,
        COUNT(sc.projection_id) AS ros_count,
        SUM(sc.deleted_row_count) AS del_rows,
        SUM(sc.delete_vector_count) AS DVC,
        MIN(pce.checkpoint_epoch) AS CPE, 
        MIN(pce.is_up_to_date) AS UTD 
    FROM
        ${VMON}.projection_storage ps
        INNER JOIN ${VCAT}.projections p USING (projection_id)
        INNER JOIN ${VMON}.storage_containers sc USING(projection_id)
        INNER JOIN ${VCAT}.projection_checkpoint_epochs pce USING(projection_id)
    WHERE p.is_segmented AND ps.projection_name LIKE '%_b0'
    GROUP BY 1
    ORDER BY 3 desc
    LIMIT 30
    ;

    \echo '    Step 7f: Catalog Analysis (Top 30 largest unsegmented projections)'
    \qecho >>> Step 7f: Catalog Analysis (Top 30 largest unsegmented projections)
    SELECT
        ps.anchor_table_schema || '.' ||  ps.anchor_table_name ||
            '.' || p.projection_name AS 'schema.table.proj' ,
        SUM(ps.row_count) AS row_count, 
        SUM(ps.used_bytes)//1024^2 AS used_bytes,
        SUM(ps.wos_row_count) AS wos_rows, 
        SUM(ps.wos_used_bytes)//1024^2 AS wos_mbytes, 
        SUM(ps.ros_row_count) AS ros_rows, 
        SUM(ps.ros_used_bytes)//1024^2 AS ros_mbytes,
        COUNT(sc.projection_id) AS ros_count,
        SUM(sc.deleted_row_count) AS del_rows,
        SUM(sc.delete_vector_count) AS DVC,
        MIN(pce.checkpoint_epoch) CPE, 
        MIN(pce.is_up_to_date) UTD 
    FROM
        ${VMON}.projection_storage ps
        INNER JOIN ${VCAT}.projections p USING (projection_id)
        INNER JOIN ${VMON}.storage_containers sc USING(projection_id)
        INNER JOIN ${VCAT}.projection_checkpoint_epochs pce USING(projection_id)
    WHERE NOT is_segmented
    GROUP BY 1
    ORDER BY 3 desc
    LIMIT 30
    ;

    \echo '    Step 7g: Catalog Analysis (Top 30 most used projections)'
    \qecho >>> Step 7g: Catalog Analysis (Top 30 most used projections)
    SELECT
        pu.anchor_table_schema || '.' ||  pu.anchor_table_name ||
            '.' || pu.projection_name AS 'schema.table.projection' ,
        COUNT(*) AS num_queries
    FROM
        v_monitor.projection_usage pu
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 30
    ;

    \echo '    Step 7h: Catalog Analysis (Top 30 less used projections)'
    \qecho >>> Step 7h: Catalog Analysis (Top 30 less used projections)
    SELECT
        pu.anchor_table_schema || '.' ||  pu.anchor_table_name ||
            '.' || pu.projection_name AS 'schema.table.projection' ,
        COUNT(*) AS num_queries
    FROM
        v_monitor.projection_usage pu
    GROUP BY 1
    ORDER BY 2
    LIMIT 30
    ;

    \echo '    Step 7i: Catalog Analysis (Tables per schema)'
    \qecho >>> Step 7i: Catalog Analysis (Tables per schema)
    SELECT
        CASE WHEN table_schema IS NULL THEN 'Grand Total' ELSE table_schema END AS Schema,  
        SUM(CASE WHEN LENGTH(partition_expression) > 0 THEN 1 ELSE 0 END) AS 'Partitioned',
        SUM(CASE WHEN LENGTH(partition_expression) = 0 THEN 1 ELSE 0 END) AS 'Not Partitioned',
        COUNT(*) AS 'Total'
    FROM 
        ${VCAT}.tables 
    GROUP BY ROLLUP(table_schema)
    ORDER BY GROUPING(table_schema)
    ;

    \echo '    Step 7j: Catalog Analysis (Catalog Size)'
    \qecho >>> Step 7j: Catalog Analysis (Catalog Size)
    SELECT
        DATE_TRUNC('day',ts) AS 'date', 
        node_name, 
        MAX(catalog_size_in_MB)::INT AS END_CATLOG_SIZE_MEM_MB
    FROM ( SELECT
            node_name, 
            TRUNC((dc_allocation_pool_statistics_by_second."time")::TIMESTAMP,'SS'::VARCHAR(2)) AS ts, 
            SUM((dc_allocation_pool_statistics_by_second.total_memory_max_value -
                 dc_allocation_pool_statistics_by_second.  free_memory_min_value))/ (1024*1024) AS catalog_size_in_MB
           FROM 
            dc_allocation_pool_statistics_by_second 
           GROUP BY 1, TRUNC((dc_allocation_pool_statistics_by_second."time")::TIMESTAMP,'SS'::VARCHAR(2)) ) foo
    GROUP BY 1,2 
    ORDER BY 1 DESC, 2;

    \echo '    Step 7k: Catalog Analysis (Number of tables)'
    \qecho >>> Step 7k: Catalog Analysis (Number of tables)
    SELECT COUNT(*) AS num_tables FROM ${VCAT}.tables ;

    \echo '    Step 7l: Catalog Analysis (Number of projections)'
    \qecho >>> Step 7l: Catalog Analysis (Number of projections)
    SELECT COUNT(*) AS num_projections FROM ${VCAT}.projections ;

    \echo '    Step 7m: Catalog Analysis (Number of projection basenames by table)'
    \qecho >>> Step 7m: Catalog Analysis (Number of projection basenames by table)
    SELECT
        n, count(*)
    FROM
        (SELECT
             anchor_table_name,
             COUNT(DISTINCT projection_basename) AS n
         FROM ${VCAT}.projections
         GROUP BY 1) x
    GROUP BY 1
    ORDER BY 2 DESC;

    \echo '    Step 7n: Catalog Analysis (Number of columns)'
    \qecho >>> Step 7n: Catalog Analysis (Number of columns)
    SELECT COUNT(*) AS num_columns FROM ${VCAT}.columns ;

    \echo '    Step 7o: Catalog Analysis (Number of delete vectors)'
    \qecho >>> Step 7o: Catalog Analysis (Number of delete vectors)
    SELECT node_name, storage_type, 
           COUNT(*) AS num_del_vectors,
           SUM(deleted_row_count) AS num_del_rows,
           SUM(used_bytes) AS used_byted
    FROM ${VMON}.delete_vectors
    GROUP BY 1, 2
    ORDER BY 1;

    \echo '    Step 7p: Catalog Analysis (Columns by statistics)'
    \qecho >>> Step 7p: Catalog Analysis (Columns by statistics)
    SELECT statistics_type, COUNT(*) AS count FROM ${VCAT}.projection_columns GROUP BY 1 ;

    \echo '    Step 7q: Catalog Analysis (Data types by encoding)'
    \qecho >>> Step 7q: Catalog Analysis (Data types by encoding)
    SELECT 
        UPPER(SPLIT_PART(data_type,'(',1)) AS data_type, 
        CASE WHEN sort_position > 0 THEN true ELSE false END AS is_sorted, 
        encoding_type, 
        COUNT(*) AS count
    FROM ${VCAT}.projection_columns
    GROUP BY 1, 2, 3 
    ORDER BY 1, 2;

    \echo '    Step 7r: Catalog Analysis (Number of storage containers)'
    \qecho >>> Step 7r: Catalog Analysis (Number of storage containers)
    SELECT node_name, COUNT(*) AS num_storage_containers
    FROM ${VMON}.storage_containers GROUP BY 1;

    -- ------------------------------------------------------------------------
    -- High-level workload definition by hour & query type
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 8a: Query_requests history analysis'
    \qecho >>> Step 8a: Query_requests history analysis
    SELECT
        MIN(start_timestamp) AS min_timestamp, 
        MAX(start_timestamp) AS max_timestamp, 
        COUNT(*) AS num_queries
    FROM 
        ${VMON}.query_requests 
    WHERE 
        is_executing is false AND
        start_timestamp BETWEEN :sdate AND :edate
    ;
    \pset expanded

    \echo '    Step 8b: Throughput by hour and Q type'
    \qecho >>> Step 8b: Throughput by hour and Q type
    SELECT
        TIME_SLICE(qr.start_timestamp, 1, 'hour','start') AS time_slice,
        CASE WHEN UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' THEN 'SELECT'
             ELSE UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) END AS qtype,
        qr.request_type ,
        COUNT(*) AS count,
        MIN(qr.request_duration_ms) AS min_ms,
        MAX(qr.request_duration_ms) AS max_ms,
        AVG(qr.request_duration_ms)::INT AS avg_ms,
        MIN(qr.memory_acquired_mb) AS min_mb,
        MAX(qr.memory_acquired_mb) AS max_mb,
        AVG(qr.memory_acquired_mb)::INT AS avg_mb
    FROM
        ${VMON}.query_requests qr
    WHERE
        qr.is_executing IS false AND
        start_timestamp BETWEEN :sdate AND :edate
    GROUP BY 
        1, 2, 3
    ORDER BY 
        1, 2, 3
    ;

    \echo '    Step 8c: Query Consumption by Resource Pool'
    \qecho >>> Step 8c: Query Consumption by Resource Pool
    WITH qc AS ( 
        SELECT
            es.cpu_cycles_us, es.network_bytes_received, es.network_bytes_sent, 
            es.data_bytes_read, es.data_bytes_written, es.data_bytes_loaded, 
            es.bytes_spilled, es.input_rows, es.input_rows_processed, es.thread_count, 
            datediff('millisecond', ri.time, rc.time) as duration_ms, 
            case when count_rp=1 then es.resource_pool else '<multiple>' end as resource_pool, 
            output_rows, request_type, success 
        FROM (
            SELECT transaction_id, statement_id, 
                   sum(cpu_cycles_us) as cpu_cycles_us, sum(network_bytes_received) as network_bytes_received, 
                   sum(network_bytes_sent) as network_bytes_sent, sum(data_bytes_read) as data_bytes_read, 
                   sum(data_bytes_written) as data_bytes_written, sum(data_bytes_loaded) as data_bytes_loaded, sum(bytes_spilled) as bytes_spilled, 
                   sum(input_rows) as input_rows, sum(input_rows_processed) as input_rows_processed, 
                   sum(thread_count) as thread_count, max(res_pool) as resource_pool, count(distinct res_pool) as count_rp 
            FROM ${VINT}.dc_execution_summaries 
            WHERE time BETWEEN :sdate AND :edate
            GROUP BY transaction_id, statement_id                                                                       
            ) es 
            LEFT OUTER JOIN 
            ( 
            SELECT transaction_id, statement_id, 
                   time, request_type, label, is_retry 
            FROM ${VINT}.dc_requests_issued 
            ) ri 
            USING (transaction_id, statement_id) 
            LEFT OUTER JOIN 
            ( 
            SELECT transaction_id, statement_id, 
                   time, processed_row_count as output_rows, success 
            FROM ${VINT}.dc_requests_completed 
            ) rc 
            USING (transaction_id, statement_id)
    )
    SELECT
        resource_pool,
        COUNT(*) AS count,
        SUM(cpu_cycles_us)//1000000 AS tot_cpu_s,
        SUM(network_bytes_received)//(1024*1024) AS net_mb_in,
        SUM(network_bytes_sent)//(1024*1024) AS net_mb_out,
        SUM(data_bytes_read)//(1024*1024) AS mbytes_read,
        SUM(data_bytes_written)//(1024*1024) AS mbytes_written,
        SUM(data_bytes_loaded)//(1024*1024) AS mbytes_loaded,
        SUM(bytes_spilled)//(1024*1024) AS mbytes_spilled,
        SUM(input_rows)//1000000 AS mrows_in,
        SUM(input_rows_processed)//1000000 AS mrows_proc,
        SUM(output_rows)//1000000 AS mrows_out,
        SUM(thread_count) AS tot_thread_count,
        SUM(duration_ms)//1000 AS tot_duration_s
    FROM
        qc
    WHERE
        request_type = 'QUERY'
    GROUP BY 
        1
    ORDER BY 
        1
    ;

    -- ------------------------------------------------------------------------
    -- Query Elapsed distribution
    -- ------------------------------------------------------------------------
    \echo '    Step 9a: Query Elapsed distribution overview'
    \qecho >>> Step 9a: Query Elapsed distribution overview
    SELECT
        ra.pool_name,
        qr.request_type,
        SUM(CASE WHEN qr.request_duration_ms < 1000 then 1 else 0 end) AS 'less1s',
        SUM(CASE WHEN qr.request_duration_ms >= 1000 AND qr.request_duration_ms < 2000 THEN 1 ELSE 0 END) AS '1to2s',
        SUM(CASE WHEN qr.request_duration_ms >= 2000 AND qr.request_duration_ms < 5000 THEN 1 ELSE 0 END) AS '2to5s',
        SUM(CASE WHEN qr.request_duration_ms >= 5000 AND qr.request_duration_ms < 10000 THEN 1 ELSE 0 END) AS '5to10s',
        SUM(CASE WHEN qr.request_duration_ms >= 10000 AND qr.request_duration_ms < 20000 THEN 1 ELSE 0 END) AS '10to20s',
        SUM(CASE WHEN qr.request_duration_ms >= 20000 AND qr.request_duration_ms < 60000 THEN 1 ELSE 0 END) AS '20to60s',
        SUM(CASE WHEN qr.request_duration_ms >= 60000 AND qr.request_duration_ms < 120000 THEN 1 ELSE 0 END) AS '1to2m',
        SUM(CASE WHEN qr.request_duration_ms >= 120000 AND qr.request_duration_ms < 300000 THEN 1 ELSE 0 END) AS '2to5m',
        SUM(CASE WHEN qr.request_duration_ms >= 300000 AND qr.request_duration_ms < 600000 THEN 1 ELSE 0 END) AS '5to10m',
        SUM(CASE WHEN qr.request_duration_ms >= 600000 AND qr.request_duration_ms < 6000000 THEN 1 ELSE 0 END) AS '10to60m',
        SUM(CASE WHEN qr.request_duration_ms >= 6000000 THEN 1 ELSE 0 END) AS 'more1h'
    FROM
        ${VMON}.query_requests qr
        INNER JOIN ${VINT}.dc_resource_acquisitions ra
        USING(transaction_id, statement_id)
    WHERE
        is_executing IS false AND
        start_timestamp BETWEEN :sdate AND :edate
    GROUP BY 1, 2
    ;

    \echo '    Step 9b: Detailed SELECT Elapsed distribution'
    \qecho >>> Step 9b: Detailed SELECT Elapsed distribution
    SELECT
        pool_name,
        bucket,
        COUNT(*) AS count
    FROM (
        SELECT
            ra.pool_name, 
            1 + qr.request_duration_ms // 1000 AS bucket
        FROM
            ${VMON}.query_requests qr
            INNER JOIN ${VINT}.dc_resource_acquisitions ra
            USING(transaction_id, statement_id)
        WHERE
            qr.request_type = 'QUERY' AND
            qr.start_timestamp BETWEEN :sdate AND :edate AND
            qr.request_duration_ms IS NOT NULL AND
            ( UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' OR
              UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) = 'SELECT' )
        ) x
    GROUP BY 1, 2
    ORDER BY 1, 2
    ;
    
    \echo '    Step 9c: Statements Execution percentile (query duration in ms)'
    \qecho >>> Step 9c: Statements Execution percentile (query duration in ms)
    SELECT DISTINCT * FROM (
        SELECT
            pool_name,
            query_type,
            query_cat,
            COUNT(*) OVER (PARTITION BY pool_name, query_type, query_cat) AS count ,
            (percentile_disc(.1) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_10  ,
            (percentile_disc(.2) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_20 ,
            (percentile_disc(.3) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_30  ,
            (percentile_disc(.4) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_40  ,
            (percentile_disc(.5) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_50  ,
            (percentile_disc(.6) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_60  ,
            (percentile_disc(.7) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_70  ,
            (percentile_disc(.8) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_80  ,
            (percentile_disc(.9) WITHIN GROUP (ORDER BY query_duration_us )
                OVER (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_90  ,
            (percentile_disc(1) WITHIN GROUP (ORDER BY query_duration_us )
                OVER  (PARTITION BY pool_name, query_type, query_cat))//1000 AS pctl_100
        FROM (
            SELECT
                ra.pool_name,
                qp.query_type,
                CASE WHEN UPPER(REGEXP_SUBSTR(qp.query, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' THEN 'SELECT'
                     ELSE UPPER(REGEXP_SUBSTR(qp.query, '\w+', 1, 1, 'b')::CHAR(8)) END AS query_cat,
                qp.query_duration_us
            FROM
                ${VMON}.query_profiles qp
                LEFT OUTER JOIN ${VINT}.dc_resource_acquisitions ra
                USING(transaction_id, statement_id)
            WHERE
                qp.query_start::TIMESTAMP BETWEEN :sdate AND :edate
        ) a
    ) b
    ORDER BY 1,2 
    ;

    \echo '    Step 9d: Statements Counts '
    \qecho >>> Step 9d: Statements Counts 
    SELECT
        ra.pool_name,
        qr.request_type,
        CASE WHEN UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) = 'WITH' THEN 'SELECT'
             ELSE UPPER(REGEXP_SUBSTR(qr.request, '\w+', 1, 1, 'b')::CHAR(8)) END AS qtype,
        COUNT(DISTINCT qr.node_name) AS nodes_data,
        COUNT(*) AS num_queries,
        MIN(qr.start_timestamp) AS Min_TS,
        MAX(qr.start_timestamp) AS Max_TS,
        SUM(qr.request_duration_ms) // 1000 As Total_Runtime_s,
        AVG(qr.request_duration_ms) // 1000 As AVG_Runtime_s,
        MIN(qr.request_duration_ms)//1000 As MIN_Runtime_s,
        MAX(qr.request_duration_ms)//1000 As MAX_Runtime_s
    FROM
        ${VMON}.query_requests qr
        INNER JOIN ${VINT}.dc_resource_acquisitions ra
        USING(transaction_id, statement_id)
    WHERE
        qr.success='t' AND
        start_timestamp BETWEEN :sdate AND :edate
    GROUP BY 1, 2, 3
    ORDER BY 1, 2
    ;

    -- ------------------------------------------------------------------------
    -- Concurrency Analysis
    -- ------------------------------------------------------------------------
    \echo '    Step 10: Query Concurrency'
    \qecho >>> Step 10: Query Concurrency
    SELECT
        qts::TIMESTAMP AS timestamp,
        request_type,
        SUM(qd) OVER (PARTITION BY request_type ORDER BY qts) concurrency
    FROM (
        SELECT  -- Each start contributes with +1
            request_type,
            start_timestamp AS qts, 
            1 AS qd
        FROM 
            ${VMON}.query_requests 
        WHERE
            start_timestamp BETWEEN :sdate AND :edate
        UNION ALL 
        SELECT  -- Each end contributes with -1
            request_type,
            end_timestamp AS qts, 
            -1 AS qd
        FROM
            ${VMON}.query_requests 
        WHERE
            start_timestamp BETWEEN :sdate AND :edate
    ) x
    ;

    -- ------------------------------------------------------------------------
    -- Epochs & Delete Vectors status
    -- ------------------------------------------------------------------------
    \echo '    Step 11a: Epoch Status'
    \qecho >>> Step 11a: Epoch Status
    SELECT 'Last Good Epoch' AS epoch, epoch_number, epoch_close_time
    FROM ${VMON}.system INNER JOIN ${VCAT}.epochs ON epoch_number = last_good_epoch 
    UNION ALL
    SELECT 'Ancient History Mark' AS epoch, epoch_number, epoch_close_time
    FROM ${VMON}.system INNER JOIN ${VCAT}.epochs ON epoch_number = ahm_epoch 
    UNION ALL
    SELECT 'Current Epoch' AS epoch, current_epoch AS epoch_number, epoch_close_time
    FROM ${VMON}.system LEFT OUTER JOIN ${VCAT}.epochs ON epoch_number = current_epoch 
    ;

    \echo '    Step 11b: Delete Vector Status'
    \qecho >>> Step 11b: Delete Vector Status
    SELECT
        start_epoch,
        end_epoch,
        storage_type,
        SUM(deleted_row_count) AS sum_deleted_rows
    FROM 
        ${VMON}.delete_vectors 
    GROUP BY 1, 2, 3;

    -- ------------------------------------------------------------------------
    -- Lock Usage
    -- ------------------------------------------------------------------------
    \echo '    Step 12a: Lock Attempts Overview'
    \qecho >>> Step 12a: Lock Attempts Overview
    SELECT
        SPLIT_PART(object_name, ':', 1) AS object,
        mode,
        result,
        description,
        COUNT(*) AS num_locks
    FROM
        ${VINT}.dc_lock_attempts
    GROUP BY 1, 2, 3, 4
    ORDER BY 5 DESC
    ;

    \echo '    Step 12b: Lock Attempts by hour/type'
    \qecho >>> Step 12b: Lock Attempts by hour/type
    SELECT
        TIME_SLICE(start_time, 1, 'hour', 'start') AS time_slice,
        CASE WHEN object=0 AND mode='X' THEN 'GCL_X'
             WHEN object=0 AND mode='S' THEN 'GCL_S'
             WHEN object=1 AND mode='X' THEN 'LCL_X'
             WHEN object=1 AND mode='S' THEN 'LCL_S'
             ELSE SPLIT_PART(object_name, ':', 1) || '_' || mode 
             END AS lock_type,
        COUNT(*) AS count
    FROM
        ${VINT}.dc_lock_attempts
    WHERE
        start_time BETWEEN :sdate AND :edate
    GROUP BY 1,2
    ORDER BY 1,2
    ;

    -- ------------------------------------------------------------------------
    -- Hardware Resource Usage
    -- ------------------------------------------------------------------------
    \echo '    Step 13a: CPU by hour'
    \qecho >>> Step 13a: CPU by hour
    SELECT
        * 
    FROM
        ${VINT}.dc_cpu_aggregate_by_hour
    WHERE
        time BETWEEN :sdate AND :edate
    ORDER BY
        time;

    \echo '    Step 13b: Memory by hour'
    \qecho >>> Step 13b: Memory by hour
    SELECT
        * 
    FROM
        ${VINT}.dc_memory_info_by_hour
    WHERE
        time BETWEEN :sdate AND :edate
    ORDER BY
        time;

    \echo '    Step 13c: Processs Info'
    \qecho >>> Step 13c: Processs Info
    SELECT
        node_name ,
        start_time ,
        process ,
        address_space_max ,
        data_size_max ,
        open_files_max ,
        threads_max ,
        files_open_max_value ,
        sockets_open_max_value ,
        other_open_max_value ,
        virtual_size_max_value ,
        resident_size_max_value ,
        shared_size_max_value ,
        text_size_max_value ,
        data_size_max_value ,
        library_size_max_value ,
        dirty_size_max_value ,
        thread_count_max_value ,
        map_count_max_value
    FROM
        ${VINT}.dc_process_info_by_hour 
    WHERE
        start_time BETWEEN :sdate AND :edate
    ORDER BY 1, 2;

    -- ------------------------------------------------------------------------
    --  TM events
    -- ------------------------------------------------------------------------
    \echo '    Step 14a: TM events'
    \qecho >>> Step 14a: TM events
    SELECT
        TIME_SLICE(time, 1, 'hour','start') AS time_slice ,
        node_name,
        operation,
        COUNT(*) AS count,
        SUM(container_count) AS containers,
        SUM(total_size_in_bytes)//1024//1024 AS size_mb
    FROM
        ${VINT}.dc_tuple_mover_events
    WHERE
        time BETWEEN :sdate AND :edate
    GROUP BY 1, 2, 3
    ORDER BY 1;

    \echo '    Step 14b: TM durations'
    \qecho >>> Step 14b: TM durations
    SELECT
        operation, 
        MIN(start_time) min_time, 
        MAX(start_time) max_time ,
        MAX(duration) ,
        MIN(duration) ,
        AVG(duration) ,
        COUNT(DISTINCT transaction_id) num_events
    FROM (
        SELECT
            start.TIME AS start_time ,
            complete.TIME AS complete_time ,
            complete.TIME - start.TIME AS duration ,
            start.node_name ,
            start.transaction_id ,
            start.operation ,
            start.event AS start_event ,
            complete.event AS complete_event
        FROM (
            SELECT * FROM ${VINT}.dc_tuple_mover_events WHERE event = 'Start') AS start
        INNER JOIN (
            SELECT * FROM ${VINT}.dc_tuple_mover_events WHERE event = 'Complete') AS complete
            USING (transaction_id, node_name)
        WHERE
            start.TIME BETWEEN :sdate AND :edate
        ORDER BY start.TIME
        ) sq
    GROUP BY 1;

    \echo '    Step 14c: Long Mergeout (> 20 mins)'
    \qecho >>> Step 14c: Long Mergeout (> 20 mins)
    SELECT
        a.node_name ,
        a.schema_name ,
        b.projection_name ,
        COUNT(*)
    FROM
        ${VINT}.dc_tuple_mover_events a 
        INNER JOIN
        ${VINT}.dc_tuple_mover_events b
        USING(transaction_id)
    WHERE
        a.event = 'Start' AND
        b.event = 'Complete' AND
        a.time BETWEEN :sdate AND :edate AND
        b.time::TIMESTAMP - a.time::TIMESTAMP > INTERVAL '20 minutes'
    GROUP BY 1, 2, 3
    ORDER BY 4 DESC
    ;

    \echo '    Step 14d: Long Running Reply Delete (> 10 mins)'
    \qecho >>> Step 14d: Long Running Reply Delete (> 10 mins)
    SELECT
        a.node_name ,
        a.schema_name ,
        b.projection_name ,
        COUNT(*)
    FROM
        ${VINT}.dc_tuple_mover_events a
        INNER JOIN
        ${VINT}.dc_tuple_mover_events b
        USING(transaction_id)
    WHERE
        a.event = 'Change plan type to Replay Delete' AND
        b.event = 'Complete' AND
        a.time BETWEEN :sdate AND :edate AND
        b.time - a.time > INTERVAL '10 minutes'
    GROUP BY 1, 2, 3
    ORDER BY 4 DESC
    ;

    -- ------------------------------------------------------------------------
    -- Script End Time
    -- ------------------------------------------------------------------------
    \echo '    Step 15: Script End Timestamp'
    \qecho >>> Step 15: Script End Timestamp
    SELECT
        SYSDATE() AS 'End Timestamp'
    ;
EOF

#---------------------------------------------------------------------------
# GZIP output file
#---------------------------------------------------------------------------
test ${GZP} -eq 1 && { echo "Gzipping ${OUT}" ; gzip -f ${OUT} ;}

#------------------------------------------------------------------------
# End of sprof
#------------------------------------------------------------------------
secs=$(( `date +%s` - secs ))
hh=$(( secs / 3600 ))
mm=$(( ( secs / 60 ) % 60 ))
ss=$(( secs % 60 ))
printf "[%s] ${SPV} completed in %d sec (%02d:%02d:%02d)\n" "`date +'%Y-%m-%d %H:%M:%S'`" ${secs} $hh $mm $ss

exit 0
