drop table if exists ods_order_detail;
create table if not exists ods_order_detail
(
    id
    string
    comment
    '编号',
    order_id
    string
    comment
    '订单编号',
    user_id
    string
    comment
    '用户id',
    sku_id
    string
    comment
    'sku_id',
    sku_name
    string
    comment
    'sku名称（冗余)',
    img_url
    string
    comment
    '图片名称（冗余)',
    order_price
    decimal
(
    10,
    2
) comment '购买价格(下单时sku价格）',
    sku_num string comment '购买个数',
    create_time string comment '创建时间'
    ) comment '订单明细表'
    partitioned by
(
    dt string
)
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ods/ods_order_detail/'
    tblproperties
(
    "parquet.compression"="gzip"
);
load
data inpath '/origin_data/db/order_detail/2025-03-24' into table ods_order_detail partition (dt='2025-03-24');
select *
from ods_order_detail;


drop table ods_order_info;
CREATE TABLE `ods_order_info`
(
    `id`           string COMMENT '编号',
    `total_amount` decimal(16, 2) COMMENT '总金额',
    `order_status` string COMMENT '订单状态',
    `user_id`      string COMMENT '用户id',
    `payment_way`  string COMMENT '订单备注',
    `out_trade_no` string COMMENT '订单交易编号（第三方支付用)',
    `create_time`  string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间'
) COMMENT'订单表 订单表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_order_info/'
tblproperties ("parquet.compression"="gzip");
load
data inpath '/origin_data/db/order_info/2025-03-24' into table ods_order_info partition (dt='2025-03-24');
select *
from ods_order_info;


CREATE TABLE `ods_user_info`
(
    `id`          string COMMENT '编号',
    `name`        string COMMENT '用户姓名',
    `birthday`    string COMMENT '用户生日',
    `gender`      string COMMENT '性别 M男,F女',
    `email`       string COMMENT '邮箱',
    `user_level`  string COMMENT '用户级别',
    `create_time` string COMMENT '创建时间'
) COMMENT '用户表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_user_info/'
tblproperties ("parquet.compression"="gzip");

load
data inpath '/origin_data/db/user_info/2025-03-24' into table ods_user_info partition (dt='2025-03-24');
select *
from ods_user_info;


CREATE TABLE `ods_sku_info`
(
    `id`           string COMMENT 'skuid(itemID)',
    `spu_id`       string COMMENT 'spuid',
    `price`        decimal(10, 0) COMMENT '价格',
    `sku_name`     string COMMENT 'sku名称',
    `sku_desc`     string COMMENT '商品规格描述',
    `weight`       decimal(10, 2) COMMENT '重量',
    `tm_id`        string COMMENT '品牌(冗余)',
    `category3_id` string COMMENT '三级分类id（冗余)',
    `create_time`  string COMMENT '创建时间'
) COMMENT'库存单元表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_sku_info/'
tblproperties ("parquet.compression"="gzip");

load
data inpath '/origin_data/db/sku_info/2025-03-24' into table ods_sku_info partition (dt='2025-03-24');
select *
from ods_sku_info;


CREATE TABLE `ods_comment_info`
(
    `id`           string COMMENT '编号',
    `user_id`      string COMMENT '用户名称',
    `sku_id`       string COMMENT 'skuid',
    `spu_id`       string COMMENT '商品id',
    `order_id`     string COMMENT '订单编号',
    `appraise`     string COMMENT '评价 1 好评 2 中评 3 差评',
    `comment_txt`  string COMMENT '评价内容',
    `create_time`  string COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间'
) COMMENT'商品评论表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_comment_info/'
tblproperties ("parquet.compression"="gzip");

load
data inpath '/origin_data/db/comment_info/2025-03-24' into table ods_comment_info partition (dt='2025-03-24');
select *
from ods_comment_info;
-----------------------------------------------------------------------------------------------------------------


--------dwd层-----------------
drop table if exists dwd_order_detail;
create table if not exists dwd_order_detail
(
    id
    string
    comment
    '编号',
    order_id
    string
    comment
    '订单编号',
    user_id
    string
    comment
    '用户id',
    sku_id
    string
    comment
    'sku_id',
    sku_name
    string
    comment
    'sku名称（冗余)',
    img_url
    string
    comment
    '图片名称（冗余)',
    order_price
    decimal
(
    10,
    2
) comment '购买价格(下单时sku价格）',
    sku_num string comment '购买个数',
    create_time string comment '创建时间'
    ) comment '订单明细表'
    partitioned by
(
    dt string
)
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dwd/dwd_order_detail/'
    tblproperties
(
    "parquet.compression"="lzo"
);
load
data inpath '/origin_data/db/order_detail/2025-03-24' into table dwd_order_detail partition (dt='2025-03-24');
select *
from dwd_order_detail;

drop table if exists dwd_sku_info;
create
external table dwd_sku_info(
id string comment 'skuId',
spu_id string comment 'spuid',
price decimal(10,2) comment '',
sku_name string comment '',
sku_desc string comment '',
weight string comment '',
tm_id string comment 'id',
category3_id string comment '1id',
create_time string comment ''
) comment ''
partitioned BY (dt string)
stored as  parquet
location '/warehouse/lxy/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="lzo");

load
data inpath '/origin_data/db/sku_info/2025-03-24' into table dwd_sku_info partition (dt='2025-03-24');
select *
from dwd_sku_info;

drop table dwd_order_info;
CREATE
external TABLE `dwd_order_info` (
  `id` string  COMMENT '编号',
  `total_amount` decimal(16,2) COMMENT '总金额',
  `order_status` string COMMENT '订单状态',
  `user_id` string COMMENT '用户id',
  `payment_way` string COMMENT '订单备注',
  `out_trade_no` string COMMENT '订单交易编号（第三方支付用)',
  `create_time` string COMMENT '创建时间',
  `operate_time` string COMMENT '操作时间'
) COMMENT'订单表 订单表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="lzo");

load
data inpath '/origin_data/db/order_info/2025-03-24' into table dwd_order_info partition (dt='2025-03-24');
select *
from dwd_order_info;


CREATE
external TABLE `dwd_user_info` (
  `id` string   COMMENT '编号',
  `name` string  COMMENT '用户姓名',
  `birthday` string  COMMENT '用户生日',
  `gender` string  COMMENT '性别 M男,F女',
  `email` string  COMMENT '邮箱',
  `user_level` string  COMMENT '用户级别',
  `create_time` string  COMMENT '创建时间'
)  COMMENT '用户表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="lzo");

load
data inpath '/origin_data/db/user_info/2025-03-24' into table dwd_user_info partition (dt='2025-03-24');
select *
from dwd_user_info;

CREATE
external TABLE `dwd_comment_info` (
  `id` string  COMMENT '编号',
  `user_id` string  COMMENT '用户名称',
  `sku_id` string  COMMENT 'skuid',
  `spu_id` string  COMMENT '商品id',
  `order_id` string  COMMENT '订单编号',
  `appraise` string  COMMENT '评价 1 好评 2 中评 3 差评',
  `comment_txt` string  COMMENT '评价内容',
  `create_time` string  COMMENT '创建时间',
  `operate_time` string  COMMENT '修改时间'
) COMMENT'商品评论表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_comment_info/'
tblproperties ("parquet.compression"="lzo");

load
data inpath '/origin_data/db/comment_info/2025-03-24' into table dwd_comment_info partition (dt='2025-03-24');
select *
from ods_comment_info;


------------dws----------------------
drop table dws_order_item_detail_wide;
create
external table dws_order_item_detail_wide(
    order_id string,
   order_detail_id string,
   user_id string,
   sku_id string,
    total_amount decimal(10,2),
    order_status string,
    payment_way string,
    out_trade_no string,
    order_price decimal(10,2),
    sku_num string,
    username string,
    birthday string,
    gender string,
    email string,
    user_level string,
    price decimal(10,2),
    sku_name string,
    sku_desc string,
    weight decimal(10,2),
    tm_id string
)partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dws/dws_order_item_detail_wide'
tblproperties ("parquet.compression"="lzo");
set
hive.exec.dynamic.partition.mode=nonstrict;
insert
overwrite table dws_order_item_detail_wide partition (dt)
select oi.id   order_id,
       od.id   order_detail_id,
       us.id   user_id,
       sk.id   sku_id,
       oi.total_amount,
       oi.order_status,
       oi.payment_way,
       oi.out_trade_no,
       od.order_price,
       od.sku_num,
       us.name username,
       us.birthday,
       us.gender,
       us.email,
       us.user_level,
       sk.price,
       sk.sku_name,
       sk.sku_desc,
       sk.weight,
       sk.tm_id,
       oi.dt
from dwd_order_info oi
         left join dwd_order_detail od on od.order_id = oi.id
         left join dwd_user_info us on oi.user_id = us.id
         left join dwd_sku_info sk on od.sku_id = sk.id
where oi.id is not null
  and od.id is not null
  and us.id is not null
  and sk.id is not null
  and oi.dt = '2025-03-24'
  and od.dt = '2025-03-24'
  and us.dt = '2025-03-24'
  and sk.dt = '2025-03-24';

use
lx;
select *
from dws_order_item_detail_wide;


drop table ads_gmv_summary;
create
external table ads_gmv_summary(
    dt string,
    gmv_count string,
    gmv_summary string,
    gmv_payment string
)row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_gmv_summary'
tblproperties ("parquet.compression"="lzo");
set
hive.exec.dynamic.partition.mode=nonstrict;


select *
from ads_gmv_summary;

create table ads_user_convent_rate
(
    dt                          string,
    total_visitor_m_count       string,
    order_u_count               string,
    visitor2order_ratio         string,
    payment_u_count             string,
    order2payment_convert_ratio string
)row format delimited fields terminated by '\t'
location  'warehouse/gmall/ads/ads_user_convent_rate'
tblproperties ("parquet.compression"="lzo");
set
hive.exec.dynamic.partition.mode=nonstrict;

select *
from ads_user_convent_rate;

create table ads_goods_purchase_rate
(
    sku_id                string,
    sku_name              string,
    buycount              string,
    buy_twice_last        string,
    buy_twice_last_ratio  string,
    buy_3times_last       string,
    buy_3times_last_ratio string,
    stat_mn               string,
    stat_date             string

) partitioned by (dt string)
row format delimited fields terminated by '\t'
location  'warehouse/gmall/ads/ads_goods_purchase_rate'
tblproperties ("parquet.compression"="lzo");
set
hive.exec.dynamic.partition.mode=nonstrict;
select *
from ads_goods_purchase_rate;