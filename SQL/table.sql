
--统计每日预警次数
create table early_warning_day_count(
  id int not null AUTO_INCREMENT,
  keyperson_type varchar,  --预警类型
  warning_count int, --预警次数
  warning_date varchar, --预警日期
  primary key(id)
);

--统计每日预警类型人员数
create table warning_type_count(
  id int not null AUTO_INCREMENT,
  keyperson_type varchar,  --预警类型
  keyperson_id varchar,  --预警人员证件号
  warning_date varchar,  --预警日期
  primary key(id)
);

--统计派出所预警人数
create table warning_police_station_count(
  id int not null AUTO_INCREMENT,
  warning_date varchar,  --预警日期
  police_station varchar,  --派出所
  keyperson_id varchar,  --证件号ID
  warning_station varchar,  --预警站点
  primary key(id)
);

--分站点统计预警量
create table warning_station_count(
  id int not null AUTO_INCREMENT,
  warning_date varchar, --预警日期
  event_address_name varchar,  --站点
  warning_count int,  --预警次数
  primary key(id)
);

--分小时统计预警量
create table warning_hour_count(
  id int not null AUTO_INCREMENT,
  warning_date varchar, --预警日期
  warning_hour varchar, --预警小时时段
  warning_count int,  --预警次数
  primary key(id)
);

--统计挂网时间分布
create table warning_timediff_count(
  id int not null AUTO_INCREMENT,
  warning_date varchar, --预警日期
  keyperson_id varchar, --证件号ID
  arrest_time_diff varchar, --挂网时长
  primary key(id)
);