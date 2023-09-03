-- this is a comment
SET pipeline.name = test-sql;
set parallelism.default = 2;
SET table.exec.mini-batch.enabled = true;
SET table.exec.mini-batch.allow-latency = 5s;
SET table.exec.mini-batch.size = 5000;
SET execution.runtime-mode = streaming;
SET execution.checkpointing.enabled = true;
set execution.checkpointing.interval = 3s;
set table.dynamic-table-options.enabled=true;


-- 源表
create table if not exists `default_catalog`.`default_database`.`tbl_aggregate_source`(
                                                                         dim string,
                                                                         user_id bigint,
                                                                         price double,
                                                                         row_time as cast(current_timestamp as timestamp(3)),
    watermark for row_time as row_time - interval '5' second  -- watermark必须为timestamp(3)类型，即精确到毫秒
    ) with (
          'connector' = 'datagen',
          'rows-per-second' = '10',
          'fields.dim.length' = '1',
          'fields.user_id.min' = '1',
          'fields.user_id.max' = '100000',
          'fields.price.min' = '50',
          'fields.price.max' = '1000'
          );

-- 结果表
create table if not exists `default_catalog`.`default_database`.`tbl_aggregate_sink`(
                                                                       dim string,
                                                                       pv bigint,
                                                                       uv bigint,
                                                                       sum_price double,
                                                                       max_price double,
                                                                       min_price double,
                                                                       window_start bigint
) with (
      'connector' = 'print',                      -- 控制台打印连接器，需要在flink web ui对应的taskmanager日志中查看
      -- 'standard-error' = 'true',               -- 是否使用标准错误输出
      -- 'sink.parallelism' = '2',                -- 并行度
      'print-identifier' = 'tbl_aggregate_sink'   -- 标识符
      );

-- 插入数据
insert into `default_catalog`.`default_database`.`tbl_aggregate_sink`
select
    dim,
    count(*) as pv,
    count(distinct user_id) as uv,
    sum(price) as sum_price,
    max(price) as max_price,
    min(price) as min_price,
    cast((unix_timestamp(cast(row_time as string))) / 60 as bigint) as window_start
from `default_catalog`.`default_database`.`tbl_aggregate_source`
group by
    dim,
    -- 将秒级时间戳转为分钟
    cast((unix_timestamp(cast(row_time as string))) / 60 as bigint);


unset pipeline.name;
unset parallelism.default;

select * from `default_catalog`.`default_database`.`tbl_aggregate_source`;