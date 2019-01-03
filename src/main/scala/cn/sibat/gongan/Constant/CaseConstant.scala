package cn.sibat.gongan.Constant

object CaseConstant{

  /***
    *预警数据表模式case
    */
  case class warningdata(id:String,device_id:String,device_type:String,device_address:String,data_sources:String,
                     keyperson_id:String,keyperson_state:String,keyperson_type:String,event_address_id:String,
                     event_address_name:String,event_status:String,compare_sources:String,image_path:String,
                     reserve:String,reserve2:String,create_time:String,update_time:String,reserve3:String,
                     convictions:String,job_name:String,name:String,pid:String,taskid:String,similarity:String,
                     position:String,data_device_type:String)

  /***
    * 撤控数据表模式case
    */
  case class examination(id:String,early_warning_id:String,early_warning_type:String,examination_approval_type:String,
                         reasons_application:String,handling_opinions:String,handling_person_code:String,
                         handling_person:String,status:String,create_user_id:String,create_time:String,
                         update_user_id:String,update_time:String,avaliable:String)

  /***
    * 入库时间表模式case
    */
  case class keyperson_base(id_number:String,NAME:String,former_name:String,foreign_name:String,sex:String,
                            birthday:String,nationality:String,NATIONAL:String,education:String,marital_status:String,
                            residence_addr:String,create_time:String,household_flag:String,nowlive:String,
                            id_number_18:String,deptid:String,zdrytype:String,zdrystate:String,globalmanage:String,
                            nickname:String,birthplace:String,currentwork:String,convictions:String,maincontrol:String,
                            deptuser:String,deptusername:String,deptname:String,rybh:String,zjzl:String,zhcs:String,
                            ch:String,sf:String,wetherxd:String,lx:String,sg:String,remark:String,gjryzt:String,
                            addperson:String,sztkh:String,phone:String,qq:String,email:String,addpersonid:String,
                            sjly:String,update_time:String,check_status:String,bankinfo:String,fxqk_hdcxdate:String,
                            fxqk_simkcj:String,fxqk_sjch:String,fxqk_telephone:String,shgx_address:String,
                            shgx_homemoble:String,shgx_name:String,shgx_relation:String,tary_idcard:String,
                            tary_lxfs:String,tary_name:String,tary_qt:String,id:String,datasource:String,
                            is_delete:String,create_user:String,submit_user:String,juris_category:String,
                            person_descript:String)

  /***
    *广州地铁数据字段
    */
  case class gzdt(card_id:String, card_type:String, deal_type:String, line:String, station_name: String, a_num:String,
                  deal_time:String, b_num:String, c_num:String,d_num:String)

  /**
    * 深圳通的数据格式
    */
  case class szt(card_id: String, deal_time:String, deal_type: String, deal_money: String, deal_value:String,
                 station_id:String, line:String, station_name:String, busCard:String, day:String)

  /***
    * 深圳通有效字段
    */
  case class sztuseful(card:String, deal_time:String, deal_type: String, station_name:String)
  case class sztwithday(card_id:String, deal_time:String, deal_type: String, station_name:String,day:String)
  /***
    * 站点和时间
    */
  case class sizeflow15min(station_name:String, deal_time:String,cnt:Long)


  /***
    *
    */
  case class linkOD(card_id:String,o_deal_time:String,o_station_name:String,d_deal_time:String,d_station_name:String,timediff:Int)
}