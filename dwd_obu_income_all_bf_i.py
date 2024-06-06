from datetime import datetime, timedelta, date, time
from pyinfra.spark import get_or_create_spark
from pyinfra import pg_handle as pg_f
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pyspark.sql import DataFrame, Window, functions as F
from dateutil.relativedelta import relativedelta


def dwd_obu_income_fin_bf_i():
    sql1 = '''
        select 
            date
            ,scene
            ,buyer
            ,price
            ,qianzhuang_mark
            ,obu_id
            ,data_type
            ,adjust_type
            ,kehuo
            ,scene2
            ,substr(date,1,7) as month
        from(


            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'激活签约' as scene2
            from(

                select 
                    to_date(obu_active_time) as date
                    ,'新办签约' as scene 
                    ,channel_signed_bank as buyer
                    ,case 
                        when substr(a.obu_active_time,1,7) >= '2024-01' and channel_signed_bank = '建行' then 70
                        else price 
                    end as price
                    ,qianzhuang as qianzhuang_mark
                    ,obu_id
                from 
                    dwd.dwd_data_tech_sign_income_ccb_sdrc_i_detail a --新办签约建行农商行
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.obu_active_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6
                union all 
                select 
                    to_date(obu_active_time) as date
                    ,'新办签约' as scene 
                    ,channel_signed_bank as buyer
                    ,case 
                        when substr(a.obu_active_time,1,7) >= '2024-01' and channel_signed_bank = '建行' then 70
                        else price 
                    end as price
                    ,qianzhuang as qianzhuang_mark
                    ,obu_id
                from 
                    dwd.dwd_data_tech_sign_income_ccb_sdrc_i_detail a --新办签约建行农商行
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.obu_active_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --首次消费
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'建行首次消费' as scene
            from(
                select 
                    to_date(first_expense_time) as date
                    ,'新办签约' as scene 
                    ,'建行-首次消费时间' as buyer
                    ,case 
                        when substr(a.first_expense_time,1,7) >= '2024-01' then 70
                        else price
                    end as price
                    ,qianzhuang_mark
                    ,obu_id
                from
                    dwd.dwd_data_tech_sign_income_ccb_consume_i_detail a 
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.first_expense_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6
                union all 
                select 
                    to_date(first_expense_time) as date
                    ,'新办签约' as scene 
                    ,'建行-首次消费时间' as buyer
                    ,case 
                        when substr(a.first_expense_time,1,7) >= '2024-01' then 70
                        else price
                    end as price
                    ,qianzhuang_mark
                    ,obu_id
                from
                    dwd.dwd_data_tech_sign_income_ccb_consume_i_detail a 
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.first_expense_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'

            union all 
            --通汇
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'通汇' as scene
            from(
                select 
                    to_date(issuedtime) as date
                    ,'新办签约' as scene 
                    ,'通汇产品' as buyer
                    ,180 as price
                    ,qianzhuang_mark
                    ,obu_id
                from 
                    dwd.dwd_data_tech_sign_income_tonghui_detail a 
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.issuedtime,1,7) = substr(now(),1,7)
                group by 1,2,3,4,5,6
                union all 
                select 
                    to_date(issuedtime) as date
                    ,'新办签约' as scene 
                    ,'通汇产品' as buyer
                    ,180 as price
                    ,qianzhuang_mark
                    ,obu_id
                from 
                    dwd.dwd_data_tech_sign_income_tonghui_detail a 
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.issuedtime,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 1,2,3,4,5,6
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --新办
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'新办收入' as scene
            from(
                select 
                    to_date(obu_active_time) as date
                    ,'新办付费' as scene 
                    ,'个人付费' as buyer
                    ,case 
                        when remark = '调减' then -180
                        when remark = '调增' then 180
                        else yingshou_amt2
                    end as price
                    ,qianzhuang_mark
                    ,obu_id
                    
                from 
                    dwd.dwd_data_tech_personal_payment_receivable_i_detail a --个人付费应收
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.obu_active_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6
                union all 
                select 
                    to_date(obu_active_time) as date
                    ,'新办付费' as scene 
                    ,'个人付费' as buyer
                    ,case 
                        when remark = '调减' then -180
                        when remark = '调增' then 180
                        else yingshou_amt2
                    end as price
                    ,qianzhuang_mark
                    ,obu_id
                    
                from 
                    dwd.dwd_data_tech_personal_payment_receivable_i_detail a --个人付费应收
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.obu_active_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --售后
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'售后收入' as scene
            from(
                select 
                    to_date(obu_active_time) as date
                    ,'售后付费' as scene 
                    ,'个人付费' as buyer
                    ,case 
                        when price = -180 then -180
                        else 180 
                    end as price
                    ,qianzhuang_mark
                    ,obu_id
                from 
                    dwd.dwd_data_tech_personal_payment_shouhou_receivable_i_detail a
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.obu_active_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6
                union all 
                select 
                    to_date(obu_active_time) as date
                    ,'售后付费' as scene 
                    ,'个人付费' as buyer
                    ,case 
                        when price = -180 then -180
                        else 180 
                    end as price
                    ,qianzhuang_mark
                    ,obu_id
                from 
                    dwd.dwd_data_tech_personal_payment_shouhou_receivable_i_detail a
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.obu_active_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6
                
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --注销
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'注销收入' as scene
            from(
                select 
                    to_date(cancel_time) as date
                    ,'注销付费' as scene 
                    ,'个人付费' as buyer 
                    ,price 
                    ,qianzhuang_mark
                    ,obu_provider_id as obu_id
                from 
                    dwd.dwd_data_tech_equipment_amount_new_i_zhuxiao_yingshou a 
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.cancel_time,1,7) = substr(now(),1,7)
                union all 
                select 
                    to_date(cancel_time) as date
                    ,'注销付费' as scene 
                    ,'个人付费' as buyer 
                    ,price 
                    ,qianzhuang_mark
                    ,obu_provider_id as obu_id
                from 
                    dwd.dwd_data_tech_equipment_amount_new_i_zhuxiao_yingshou a 
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.cancel_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --减免
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'减免数据' as scene
            from(
                select 
                    to_date(obu_active_time) as date
                    ,'减免数据' as scene 
                    ,newold as buyer 
                    ,0 as price 
                    ,qianzhuang_mark
                    ,obu_id
                from 
                    dwd.dwd_data_tech_income_free_i a 
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.obu_active_time,1,7) = substr(now(),1,7)
                    and department = '数据科技事业部'
                union all 
                select 
                    to_date(obu_active_time) as date
                    ,'减免数据' as scene 
                    ,newold as buyer 
                    ,0 as price 
                    ,qianzhuang_mark
                    ,obu_id
                from 
                    dwd.dwd_data_tech_income_free_i a 
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.obu_active_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                    and department = '数据科技事业部'
            )tmp1 

            union all 

            --单换卡
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'单换卡实收' as scene
            from (
                select 
                    to_date(gmt_payment) as date
                    ,'单换卡' as scene
                    ,'单换卡' as buyer
                    ,equipment_amount as price
                    ,qianzhuang_mark
                    ,coalesce(obu_provider_id,id) as obu_id
                from 
                    dwd.dwd_data_tech_equipment_amount_new_i
                where 
                    pay_state = 'SUCCESS'
                    and newold_detail = '更换卡'
                    and department = '数据科技事业部' --归属事业部
                    and sale_department = '数据科技事业部' --销售部门
                    and substr(to_date(gmt_payment),1,7) >= '2023-10'
            )tmp1 
            union all 
            -------调整数据
            --签约调增
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'货车' as kehuo 
                ,'签约调增' as scene
            from (
                select 
                    qianzhuang_mark
                    ,to_date(sign_time) as date 
                    ,'新办签约' as scene
                    ,channel_signed_bank as buyer
                    ,'调增' as adjust_type
                    ,price as price
                    ,obu_id
                from 
                    dwd.dwd_data_tech_sign_income_ccb_sdrc_increase_i_detail  a --签约应收调增
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.sign_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6,7
                union all 
                select 
                    qianzhuang_mark
                    ,to_date(sign_time) as date 
                    ,'新办签约' as scene
                    ,channel_signed_bank as buyer
                    ,'调增' as adjust_type
                    ,price as price
                    ,obu_id
                from 
                    dwd.dwd_data_tech_sign_income_ccb_sdrc_increase_i_detail  a --签约应收调增
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.sign_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6,7
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --新办调增
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'货车' as kehuo 
                ,'新办个人调增' as scene
            from(
                select 
                    qianzhuang_mark
                    ,to_date(gmt_unbound) as date 
                    ,'新办付费' as scene
                    ,'个人付费' as buyer
                    ,'调增' as adjust_type
                    ,equipment_amount as price
                    ,obu_provider_id as obu_id 
                from 
                    dwd.dwd_data_tech_personal_payment_receivable_increase_i_detail  a --个人付费应收调增
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.gmt_unbound,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6,7
                union all 
                select 
                    qianzhuang_mark
                    ,to_date(gmt_unbound) as date 
                    ,'新办付费' as scene
                    ,'个人付费' as buyer
                    ,'调增' as adjust_type
                    ,equipment_amount as price
                    ,obu_provider_id as obu_id 
                from 
                    dwd.dwd_data_tech_personal_payment_receivable_increase_i_detail  a --个人付费应收调增
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.gmt_unbound,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6,7
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --签约调减
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'货车' as kehuo 
                ,'签约调减' as scene
            from(
                select 
                    qianzhuang_mark
                    ,to_date(rescind_date) as date 
                    ,'新办签约' as scene
                    ,'建行-首次消费时间' as buyer
                    ,'调减' as adjust_type
                    ,0 - 70 as price
                    ,case 
                        when a.sign_time < '2022-10-01' then a.vehicle_id
                        when a.sign_time >= '2022-10-01' and mark = '有首次消费' then a.vehicle_id
                    end as obu_id 
                from 
                    dwd.dwd_data_tech_sign_income_ccb_sdrc_decrease_i_detail a --签约收入调减
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.rescind_date,1,7) = substr(now(),1,7)
                    and case 
                        when a.sign_time < '2022-10-01' then a.vehicle_id
                        when a.sign_time >= '2022-10-01' and mark = '有首次消费' then a.vehicle_id
                    end is not null 
                group by 
                    1,2,3,4,5,6,7
                union all 
                select 
                    qianzhuang_mark
                    ,to_date(rescind_date) as date 
                    ,'新办签约' as scene
                    ,'建行-首次消费时间' as buyer
                    ,'调减' as adjust_type
                    ,0 - 70 as price
                    ,case 
                        when a.sign_time < '2022-10-01' then a.vehicle_id
                        when a.sign_time >= '2022-10-01' and mark = '有首次消费' then a.vehicle_id
                    end as obu_id 
                from 
                    dwd.dwd_data_tech_sign_income_ccb_sdrc_decrease_i_detail a --签约收入调减
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.rescind_date,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                    and case 
                        when a.sign_time < '2022-10-01' then a.vehicle_id
                        when a.sign_time >= '2022-10-01' and mark = '有首次消费' then a.vehicle_id
                    end is not null 
                group by 
                    1,2,3,4,5,6,7
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'

            union all 
            --新办调减
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'货车' as kehuo 
                ,'新办个人调减' as scene
            from(
                select 
                    qianzhuang_mark
                    ,to_date(decrease_time) as date 
                    ,'新办付费' as scene
                    ,'个人付费' as buyer
                    ,'调减' as adjust_type
                    ,0 - decrease_money as price
                    ,obu_id
                from 
                    dwd.dwd_data_tech_personal_payment_receivable_decrease_i_detail a --个人付费应收调减
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.decrease_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6,7
                union all 
                select 
                    qianzhuang_mark
                    ,to_date(decrease_time) as date 
                    ,'新办付费' as scene
                    ,'个人付费' as buyer
                    ,'调减' as adjust_type
                    ,0 - decrease_money as price
                    ,obu_id
                from 
                    dwd.dwd_data_tech_personal_payment_receivable_decrease_i_detail a --个人付费应收调减
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.decrease_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6,7
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --售后调减
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'货车' as kehuo 
                ,'售后个人调减' as scene
            from(
                select 
                    qianzhuang_mark
                    ,to_date(decrease_time) as date 
                    ,'售后付费' as scene
                    ,'个人付费' as buyer
                    ,'调减' as adjust_type
                    ,0 - decrease_money as price
                    ,obu_id
                from
                    dwd.dwd_data_tech_personal_payment_receivable_decrease_i_shouhou_detail  a --个人付费应收调减
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.decrease_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6,7
                union all 
                select 
                    qianzhuang_mark
                    ,to_date(decrease_time) as date 
                    ,'售后付费' as scene
                    ,'个人付费' as buyer
                    ,'调减' as adjust_type
                    ,0 - decrease_money as price
                    ,obu_id
                from
                    dwd.dwd_data_tech_personal_payment_receivable_decrease_i_shouhou_detail  a --个人付费应收调减
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.decrease_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6,7
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'

            union all 
            --注销调减
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'货车' as kehuo 
                ,'注销个人调减' as scene
            from(
                select 
                    qianzhuang_mark
                    ,to_date(yingtui_time) as date 
                    ,'注销收费' as scene
                    ,'个人付费' as buyer
                    ,'调减' as adjust_type
                    ,0 - equipment_amount as price
                    ,coalesce(obu_id_new,obu_provider_id) as obu_id
                from 
                    dwd.dwd_data_tech_equipment_amount_new_i_zhuxiao_yingshou_tiaojian a
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(a.yingtui_time,1,7) = substr(now(),1,7)
                union all 
                select 
                    qianzhuang_mark
                    ,to_date(yingtui_time) as date 
                    ,'注销收费' as scene
                    ,'个人付费' as buyer
                    ,'调减' as adjust_type
                    ,0 - equipment_amount as price
                    ,coalesce(obu_id_new,obu_provider_id) as obu_id
                from 
                    dwd.dwd_data_tech_equipment_amount_new_i_zhuxiao_yingshou_tiaojian a
                where 
                    substr(ds,7,2) = '01'
                    and substr(a.yingtui_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'

            
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,'调减' as adjust_type
                ,'货车' as kehuo 
                ,'单换卡实收' as scene
            from (
                select 
                    to_date(gmt_refund) as date
                    ,'单换卡' as scene
                    ,'单换卡' as buyer
                    ,0 - equipment_amount as price
                    ,qianzhuang_mark
                    ,coalesce(obu_provider_id,id) as obu_id
                from 
                    dwd.dwd_data_tech_equipment_amount_new_i
                where 
                    refund_state = 'SUCCESS'
                    and newold_detail = '更换卡'
                    and department = '数据科技事业部' --归属事业部
                    and sale_department = '数据科技事业部' --销售部门
                    and substr(gmt_refund,1,7) >= '2023-10'
            )tmp1 
            union all 
            select 
                to_date(obu_active_time) as date
                ,'全量obu' as scene
                ,newold as buyer
                ,0 as price
                ,qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'货车' as kehuo 
                ,'全量obu' as scene
            from 
                dwd.dwd_data_tech_all_obu_new_f
            where 
                ds = date_format(now(),'yyyyMMdd')
                and substr(obu_active_time,1,7) >= '2023-10'
                and department = '数据科技事业部'
        )
        where 
            substr(date,1,7) >= substr(date_add(now(),-1),1,7) --'2023-10'

    '''
    
    spark.sql(sql1).write.mode("overwrite").insertInto('dwd.dwd_obu_income_fin_bf_i')


def dwd_obu_income_etc_bf_init():
    sql1 = '''
        select 
            date
            ,scene
            ,buyer
            ,price
            ,qianzhuang_mark
            ,obu_id
            ,data_type
            ,adjust_type
            ,kehuo
            ,scene2
            ,substr(date,1,7) as month
        from(
            --7-9月
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,case 
                    when scene2 = '注销收入' then coalesce(b.obu_id,a.obu_id)
                    else a.obu_id
                end as obu_id
                ,data_type
                ,adjust_type
                ,kehuo
                ,scene2
            from 
                dwd.dwd_obu_income_all_bf_i a 
            left join (
                select 
                    id
                    ,coalesce(a.obu_id,c.obuId) as obu_id
                from 
                    dwd.dwd_etc_financial_detail_f a 
                left join (
                    select 
                        t1.orderno
                        ,t1.orderId
                        ,t1.plateNo
                        ,t1.name
                        ,t1.obuId
                    from 
                        ods.etc_bus_cancellation t1 
                )c
                on a.service_order_id = c.orderId
                where 
                    ds = '20231101'
            )b 
            on a.obu_id = b.id
            where  
                kehuo = '客车'
                and substr(to_date(date),1,7) >= '2023-07'
                and substr(to_date(date),1,7) <= '2023-09'
            union all 
            --1-6月
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,case 
                    when scene2 = '注销收入' then coalesce(b.obu_id,a.obu_id)
                    else a.obu_id
                end as obu_id
                ,data_type
                ,adjust_type
                ,kehuo
                ,scene2
            from(


                select 
                    to_date(operation_time) as date
                    ,'买断' as scene 
                    ,'收货方' as buyer
                    ,0 as price 
                    ,null as qianzhuang_mark
                    ,obu_id
                    ,'应收数据' as data_type 
                    ,'应收数据' as adjust_type
                    ,'客车' as kehuo 
                    ,'买断' as scene2
                from 
                    bigdata.cdx_etc_unit_obu_buyout_bak_0829
            
                union all 

                select distinct
                    to_date(operation_time) as date
                    ,'预付' as scene 
                    ,'预付方' as buyer
                    ,case 
                        when to_date(operation_time) <= '2023-06-30' and operation_income = 83 then 78 
                        else operation_income 
                    end as price
                    ,null as qianzhuang_mark
                    ,obu_id
                    ,'应收数据' as data_type 
                    ,'应收数据' as adjust_type
                    ,'客车' as kehuo 
                    ,'预付' as scene2
                    
                from 
                    bigdata.cdx_etc_unit_obu_payment_bak_0829
                


                union all 
                select distinct 
                    issuedtime as date
                    ,'新办签约' as scene 
                    ,bank_name as buyer
                    ,money
                    ,null as qianzhuang_mark
                    ,obuid
                    ,'应收数据' as data_type 
                    ,'应收数据' as adjust_type
                    ,'客车' as kehuo 
                    ,'激活签约' as scene
                    -- ,case 
                    --     when bank_name like '%建%' then '中国建设银行'
                    --     else bank_name 
                    -- end as kehu
                    
                from 
                    dwd.dwd_etc_equipment_amount_yecai_i
                where 
                    ds = '20230831'
                    and money != 0 
                    and nvl(openname,'1') not like '%前装%'



                union all 
                select 
                    to_date(obu_should_time) as issuedtime
                    ,'新办付费' as scene
                    ,'个人付费' as buyer
                    ,price
                    ,null as qianzhuang_mark
                    ,obu_id
                    ,'应收数据' as data_type 
                    ,'应收数据' as adjust_type
                    ,'客车' as kehuo 
                    ,'新办收入' as scene
                from 
                    dwd.dwd_etc_financial_detail_xinban_yingshou
                where 
                    ds = '20230831'

                union all 
                select 
                    to_date(issuedtime) as issuedtime
                    ,'售后付费' as scene
                    ,'个人付费' as buyer
                    ,price
                    ,null as qianzhuang_mark
                    ,obu_id
                    ,'应收数据' as data_type 
                    ,'应收数据' as adjust_type
                    ,'客车' as kehuo 
                    ,'售后收入' as scene
                from 
                    dwd.dwd_etc_financial_detail_shouhou_yingshou
                where 
                    ds = '20230831'


                union all
                --注销收入
                select
                    date
                    ,scene
                    ,buyer
                    ,price
                    ,null as qianzhuang_mark
                    ,obu_id
                    ,'应收数据' as data_type 
                    ,'应收数据' as adjust_type
                    ,'客车' as kehuo 
                    ,'注销收入' as scene
                from(
                    
                    select 
                        to_date(issuedtime) as date
                        ,'注销付费' as scene
                        ,'个人付费' as buyer 
                        ,should_amount as price
                        ,obu_id
                    from 
                        dwd.dwd_etc_financial_detail_zhuxiao_yingshou
                    where 
                        ds = '20230831'
                
                    
                )tmp1 
                where 
                    substr(to_date(date),1,7) >= '2023-01'
                    and substr(to_date(date),1,7) <= '2023-06'
                union all 
                select
                    date
                    ,scene
                    ,buyer
                    ,price
                    ,null as qianzhuang_mark
                    ,obu_id
                    ,'应收数据' as data_type 
                    ,'应收数据' as adjust_type
                    ,'客车' as kehuo 
                    ,'建行首次消费' as scene
                from(
                
                    select 
                        to_date(first_expense_time) as date
                        ,'新办签约' as scene 
                        ,'建行-首次消费时间' as buyer
                        ,obuid as obu_id
                        ,80 as price
                    from 
                        bigdata.cdx_dwd_etc_equipment_amount_yecai_i_bak_0829
                    where 
                        bank_name in('建行-2.99','建行-50','中国建设银行')
                        and nvl(openname,'1') not like '%前装%'
                        and money != 0
                )tmp1 
                where 
                    substr(to_date(date),1,7) >= '2023-01'
                    and substr(to_date(date),1,7) <= '2023-06'

                union all 
                select 
                    date
                    ,scene
                    ,case 
                        when kh = '客车-新个人企业承担' then '个人付费'
                        when kh = '客车-建设银行' then '中国建设银行'
                        else kh
                    end as buyer
                    ,price
                    ,null as qianzhuang_mark
                    ,obuid as obu_id
                    ,'调整数据' as data_type 
                    ,case 
                        when price > 0 then '调增'
                        when price < 0 then '调减'
                    end as adjust_type
                    ,'客车' as kehuo 
                    ,case 
                        when kh = '客车-建设银行' then '签约调减'
                        when scene = '新办付费' then '新办个人调减'
                        when scene = '售后付费' then '售后个人调减'
                        when scene = '注销付费' then '注销个人调减'
                    end as scene2
                from 
                    bigdata.cdx_shenji_tiaojian_2_1020
                where 
                    case 
                        when mark1 = '建行调增' then 1
                        when obuid = '3701082224452844' then 0
                        when kh = '客车-建设银行(前装)' then 1
                        when department2 = '客车' and qianzhuang_mark = '前装' then 0 
                        when kh = '货车-建设银行' and price > 0 then 0
                        when kh = '货车农商行' then 0
                        else 1 
                    end = 1
                    and department2 = '客车'
                    and scene != '前装'
            )a
            left join (
                select 
                    id
                    ,coalesce(a.obu_id,c.obuId) as obu_id
                from 
                    dwd.dwd_etc_financial_detail_f a 
                left join (
                    select 
                        t1.orderno
                        ,t1.orderId
                        ,t1.plateNo
                        ,t1.name
                        ,t1.obuId
                    from 
                        ods.etc_bus_cancellation t1 
                )c
                on a.service_order_id = c.orderId
                where 
                    ds = '20231101'
            )b 
            on a.obu_id = b.id
            where 
                substr(to_date(date),1,7) >= '2023-01'
                and substr(to_date(date),1,7) <= '2023-06'
        )tmp1 

    '''
    #spark.sql(sql1).write.partitionBy('month').saveAsTable('dwd.dwd_obu_income_etc_bf_i',mode = 'overwrite')


def dwd_obu_income_etc_bf_i():
    sql1 = '''
        select 
            date
            ,scene
            ,buyer
            ,price
            ,qianzhuang_mark
            ,obu_id
            ,data_type
            ,adjust_type
            ,kehuo
            ,scene2
            ,substr(date,1,7) as month
        from(
            --etc事业部
            --新办签约
            select
                date
                ,scene
                ,buyer
                ,price
                ,'' as qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'客车' as kehuo 
                ,'激活签约' as scene2
            from(
                select 
                    issuedtime as date
                    ,'新办签约' as scene 
                    ,bank_name as buyer
                    ,money as price
                    ,obuid as obu_id
                    
                from 
                    dwd.dwd_etc_financial_detail_qianyue_yingshou
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(issuedtime,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5
                union all 
                select 
                    issuedtime as date
                    ,'新办签约' as scene 
                    ,bank_name as buyer
                    ,money as price
                    ,obuid as obu_id
                    
                from 
                    dwd.dwd_etc_financial_detail_qianyue_yingshou
                where 
                    substr(ds,7,2) = '01'
                    and substr(issuedtime,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5
                
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --建行首次消费
            select
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'客车' as kehuo 
                ,'建行首次消费' as scene
            from(
                select 
                    to_date(first_expense_time) as date
                    ,'新办签约' as scene 
                    ,'建行-首次消费时间' as buyer
                    ,case when source = '线下导入' then money
                        when substr(first_expense_time,1,7) >= '2024-01' then 70
                        else 80
                        end as price 
                    ,obuid as obu_id 
                from 
                    dwd.dwd_etc_financial_detail_qianyue_shoucixiaofei_yingshou
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(first_expense_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5
                union all 
                select 
                    to_date(first_expense_time) as date
                    ,'新办签约' as scene 
                    ,'建行-首次消费时间' as buyer
                    ,case when source = '线下导入' then money
                    when substr(first_expense_time,1,7) >= '2024-01' then 70
                        else 80
                        end as price 
                    ,obuid as obu_id 
                from 
                    dwd.dwd_etc_financial_detail_qianyue_shoucixiaofei_yingshou
                where 
                    substr(ds,7,2) = '01'
                    and substr(first_expense_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --新办收入
            select
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'客车' as kehuo 
                ,'新办收入' as scene
            from(
                
                select 
                    to_date(obu_should_time) as date
                    ,'新办付费' as scene
                    ,'个人付费' as buyer 
                    ,price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_xinban_yingshou
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(obu_should_time,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5
                union all 
                select 
                    to_date(obu_should_time) as date
                    ,'新办付费' as scene
                    ,'个人付费' as buyer 
                    ,price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_xinban_yingshou
                where 
                    substr(ds,7,2) = '01'
                    and substr(obu_should_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5
                
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --售后收入
            select
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'客车' as kehuo 
                ,'售后收入' as scene
            from(
                
                select 
                    to_date(issuedtime) as date
                    ,'售后付费' as scene
                    ,'个人付费' as buyer 
                    ,price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_shouhou_yingshou
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(issuedtime,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5
                union all 
                select 
                    to_date(issuedtime) as date
                    ,'售后付费' as scene
                    ,'个人付费' as buyer 
                    ,price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_shouhou_yingshou
                where 
                    substr(ds,7,2) = '01'
                    and substr(issuedtime,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5
                
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all
            --注销收入
            select
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'客车' as kehuo 
                ,'注销收入' as scene
            from(
                
                select 
                    to_date(issuedtime) as date
                    ,'注销付费' as scene
                    ,'个人付费' as buyer 
                    ,should_amount as price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_zhuxiao_yingshou
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(issuedtime,1,7) = substr(now(),1,7)
                group by 1,2,3,4,5
                union all 
                select 
                    to_date(issuedtime) as date
                    ,'注销付费' as scene
                    ,'个人付费' as buyer 
                    ,should_amount as price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_zhuxiao_yingshou
                where 
                    substr(ds,7,2) = '01'
                    and substr(issuedtime,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 1,2,3,4,5
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'

            union all 
            --买断
            select
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'客车' as kehuo 
                ,'买断' as scene
            from(
                select 
                    to_date(operation_time) as date
                    ,'买断' as scene 
                    ,concat(recipient_org_name,'|',recipient_dept) as buyer
                    ,operation_income as price
                    ,obu_id
                
                from 
                    dwd.etc_unit_obu_buyout
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(operation_time,1,7) = substr(now(),1,7)
                group by 1,2,3,4,5
                union all 
                select 
                    to_date(operation_time) as date
                    ,'买断' as scene 
                    ,concat(recipient_org_name,'|',recipient_dept) as buyer
                    ,operation_income as price
                    ,obu_id
                
                from 
                    dwd.etc_unit_obu_buyout
                where 
                    substr(ds,7,2) = '01'
                    and substr(operation_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 1,2,3,4,5
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --预付
            select
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'应收数据' as data_type 
                ,'应收数据' as adjust_type
                ,'客车' as kehuo 
                ,'预付' as scene
            from(
                
                select 
                    to_date(operation_time) as date
                    ,'预付' as scene 
                    ,'预付方' as buyer
                    ,operation_income as price
                    ,obu_id
                    
                from 
                    dwd.etc_unit_obu_payment
                where 
                    case 
                        when to_date(operation_time) <= '2023-06-30' then 1
                        when firstAgentName  like '%测试%' then 0 
                        else 1 
                    end = 1
                    and ds = date_format(now(),'yyyyMMdd')
                    and substr(operation_time,1,7) = substr(now(),1,7)
                    
                group by 1,2,3,4,5
                union all 
                select 
                    to_date(operation_time) as date
                    ,'预付' as scene 
                    ,'预付方' as buyer
                    ,operation_income as price
                    ,obu_id
                    
                from 
                    dwd.etc_unit_obu_payment
                where 
                    case 
                        when to_date(operation_time) <= '2023-06-30' then 1
                        when firstAgentName  like '%测试%' then 0 
                        else 1 
                    end = 1
                    and substr(ds,7,2) = '01'
                    and substr(operation_time,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                    
                group by 1,2,3,4,5
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'

            union all 
            --签约调减
            select 
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'客车' as kehuo 
                ,'签约调减' as scene
            from(
                select 
                    to_date(date) as date
                    ,'新办签约' as scene
                    ,bank_name as buyer
                    ,'调减' as adjust_type 
                    ,price
                    ,obuid as obu_id
                from 
                    dwd.dwd_etc_financial_detail_qianyue_tiaojian 
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(date,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6
                union all 
                select 
                    to_date(date) as date
                    ,'新办签约' as scene
                    ,bank_name as buyer
                    ,'调减' as adjust_type 
                    ,price
                    ,obuid as obu_id
                from 
                    dwd.dwd_etc_financial_detail_qianyue_tiaojian 
                where 
                    substr(ds,7,2) = '01'
                    and substr(date,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6
            
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            --新办个人调减
            select 
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'客车' as kehuo 
                ,'新办个人调减' as scene
            from(

                select 
                    to_date(date) as date
                    ,'新办付费' as scene
                    ,'个人付费' as buyer 
                    ,'调减' as adjust_type
                    ,should_amount as price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_xinban_yingshou_tiaojian
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(date,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6
                union all 
                select 
                    to_date(date) as date
                    ,'新办付费' as scene
                    ,'个人付费' as buyer 
                    ,'调减' as adjust_type
                    ,should_amount as price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_xinban_yingshou_tiaojian
                where 
                    substr(ds,7,2) = '01'
                    and substr(date,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'

            union all 
            select 
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'客车' as kehuo 
                ,'售后个人调减' as scene
            from(

                select 
                    to_date(date) as date
                    ,'售后付费' as scene
                    ,'个人付费' as buyer 
                    ,'调减' as adjust_type
                    ,should_amount as price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_shouhou_yingshou_tiaojian
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(date,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6
                union all 
                select 
                    to_date(date) as date
                    ,'售后付费' as scene
                    ,'个人付费' as buyer 
                    ,'调减' as adjust_type
                    ,should_amount as price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_shouhou_yingshou_tiaojian
                where 
                    substr(ds,7,2) = '01'
                    and substr(date,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6

            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 

            select 
                date
                ,scene
                ,buyer
                ,price
                ,null as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,adjust_type
                ,'客车' as kehuo 
                ,'注销个人调减' as scene
            from(
                select 
                    to_date(date) as date
                    ,'注销付费' as scene
                    ,'个人付费' as buyer 
                    ,'调减' as adjust_type
                    ,should_amount as price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_zhuxiao_yingshou_tiaojian
                where 
                    ds = date_format(now(),'yyyyMMdd')
                    and substr(date,1,7) = substr(now(),1,7)
                group by 
                    1,2,3,4,5,6
                union all 
                select 
                    to_date(date) as date
                    ,'注销付费' as scene
                    ,'个人付费' as buyer 
                    ,'调减' as adjust_type
                    ,should_amount as price
                    ,obu_id
                from 
                    dwd.dwd_etc_financial_detail_zhuxiao_yingshou_tiaojian
                where 
                    substr(ds,7,2) = '01'
                    and substr(date,1,7) = substr(add_months(to_date(ds,'yyyyMMdd'),-1),1,7)
                    and ds >= '20231101'
                group by 
                    1,2,3,4,5,6
            )tmp1 
            where 
                substr(to_date(date),1,7) >= '2023-10'
            union all 
            select 
                to_date(date)
                ,case 
                    when scene = '客车新办付费' then '新办付费'
                    when scene = '客车注销付费' then '注销付费'
                    when scene = '客车售后付费' then '售后付费'
                end as scene
                ,'个人付费' as buyer
                ,money as price
                ,null as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,'调减' as adjust_type
                ,'客车' as kehuo 
                ,case 
                    when scene = '客车新办付费' then '新办个人调减'
                    when scene = '客车注销付费' then '注销个人调减'
                    when scene = '客车售后付费' then '售后个人调减'
                end as scene
            from 
                bigdata.cdx_dwd_obu_income_all_bf_i_tiaozheng_zhibojian0126
            union all 
            select 
                to_date(date)
                ,case 
                    when scene2 = '客车新办付费' then '新办付费'
                    when scene2 = '客车注销付费' then '注销付费'
                    when scene2 = '客车售后付费' then '售后付费'
                end as scene
                ,'个人付费' as buyer
                ,price as price
                ,null as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,'调减' as adjust_type
                ,'客车' as kehuo 
                ,case 
                    when scene2 = '客车新办付费' then '新办个人调减'
                    when scene2 = '客车注销付费' then '注销个人调减'
                    when scene2 = '客车售后付费' then '售后个人调减'
                end as scene
            from 
                bigdata.cdx_dwd_obu_income_all_bf_i_tiaozheng_gerenshouru0126
        )t1 
        where 
            substr(date,1,7) >= substr(date_add(now(),-1),1,7)
    '''
    spark.sql(sql1).write.mode("overwrite").insertInto('dwd.dwd_obu_income_etc_bf_i')

def dwd_obu_income_all_bf_i():
    sql1 = '''
        select 
            date
            ,scene
            ,buyer
            ,price
            ,qianzhuang_mark
            ,obu_id
            ,data_type
            ,adjust_type
            ,kehuo
            ,scene2
            ,case 
                when kehuo = '客车' and scene = '新办签约' and price = 80 then 50/1.13+30/1.06
                when kehuo = '客车' and scene = '新办签约' and price = -80 then -50/1.13-30/1.06
                when kehuo = '客车' and scene = '新办签约' and price = 70 then 50/1.13+20/1.06
                when kehuo = '客车' and scene = '新办签约' and price = -70 then -50/1.13-20/1.06
                when kehuo = '客车' and scene = '新办签约' and price = 150 then 94/1.13+56/1.06
                when kehuo = '客车' and scene = '新办签约' and price = -150 then -94/1.13-56/1.06
                when kehuo = '客车' and scene = '新办签约' and price = 130 then 94/1.13+36/1.06
                when kehuo = '客车' and scene = '新办签约' and price = -130 then -94/1.13-36/1.06
                when kehuo = '客车' and scene = '新办签约' and price = 180 then 94/1.13+86/1.06
                when kehuo = '客车' and scene = '新办签约' and price = -180 then -94/1.13-86/1.06
                when kehuo = '客车' and scene = '新办签约' and price = 195 then 94/1.13+101/1.06
                when kehuo = '客车' and scene = '新办签约' and price = -195 then -94/1.13-116/1.06
                when kehuo = '客车' and scene = '新办签约' and price = 210 then 94/1.13+116/1.06
                when kehuo = '客车' and scene = '新办签约' and price = -210 then -94/1.13-116/1.06
                when kehuo = '客车' and scene = '新办付费' and price = 180 then 94/1.13+86/1.06
                when kehuo = '客车' and scene = '新办付费' and price = -180 then -94/1.13-86/1.06
                when kehuo = '客车' and scene = '新办付费' and price != 180 then price/1.06
                when kehuo = '客车' and scene = '预付' then 73/1.06
                else 0 
            end as shuihou_price
            ,month
        from (
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,data_type
                ,adjust_type
                ,kehuo
                ,scene2
                ,0 as shuihou_price
                ,month
                
            from 
                dwd.dwd_obu_income_fin_bf_i
            union all 
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,data_type
                ,adjust_type
                ,kehuo
                ,scene2
                ,case 
                    when scene = '新办签约' and price = 80 then 50/1.13+30/1.06
                    when scene = '新办签约' and price = -80 then -50/1.13-30/1.06
                    when scene = '新办签约' and price = 70 then 50/1.13+20/1.06
                    when scene = '新办签约' and price = -70 then -50/1.13-20/1.06
                    when scene = '新办签约' and price = 150 then 94/1.13+56/1.06
                    when scene = '新办签约' and price = -150 then -94/1.13-56/1.06
                    when scene = '新办签约' and price = 130 then 94/1.13+36/1.06
                    when scene = '新办签约' and price = -130 then -94/1.13-36/1.06
                    when scene = '新办签约' and price = 180 then 94/1.13+86/1.06
                    when scene = '新办签约' and price = -180 then -94/1.13-86/1.06
                    when scene = '新办签约' and price = 195 then 94/1.13+101/1.06
                    when scene = '新办签约' and price = -195 then -94/1.13-116/1.06
                    when scene = '新办签约' and price = 210 then 94/1.13+116/1.06
                    when scene = '新办签约' and price = -210 then -94/1.13-116/1.06
                    when scene = '新办付费' and price = 180 then 94/1.13+86/1.06
                    when scene = '新办付费' and price = -180 then -94/1.13-86/1.06
                    when scene = '新办付费' and price != 180 then price/1.06
                    when scene = '预付' then 73/1.06
                    else 0 
                end as shuihou_price
                ,month
            from 
                dwd.dwd_obu_income_etc_bf_i
            where 
                obu_id is not null 
            union all 
            select 
                *
            from 
                bigdata.cdx_obu_yecai_jiesuanwenjian_2401_tiaozhengmingxi --23年4季度调整明细
            union all 
            select 
                *
            from 
                bigdata.cdx_nongshang_tiaozhengmingxi_20241jidu2
            union all 
            select 
                to_date('2024-01-06') date
                ,'新办签约'	scene
                ,'山东工行一类户'	buyer
                ,-180	price
                ,'非前装'	qianzhuang_mark
                ,'3701084321554182'	obu_id
                ,'调整数据'	data_type
                ,'调减'	adjust_type
                ,'货车'	kehuo
                ,'调减24年签约23年4季度结算的数据'	scene2
                ,0	shuihou_price
                ,'2024-01' as month
            union all 
            select 
                to_date(date)
                ,case 
                    when scene2 = '客车新办付费' then '新办付费'
                    when scene2 = '客车注销付费' then '注销付费'
                    when scene2 = '客车售后付费' then '售后付费'
                end as scene
                ,'个人付费' as buyer
                ,price as price
                ,null as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,'调减' as adjust_type
                ,'客车' as kehuo 
                ,'客车个人付费1-6月调减' as scene
                ,0 as shuihou_price
                ,'2023-12' as month
            from 
                bigdata.cdx_dwd_obu_income_all_bf_i_tiaozheng_gerenshouru0226
            union all 
            select 
                to_date('2023-12-31') as date
                ,'新办签约' as scene
                ,'工商银行' as buyer
                ,price as price
                ,null as qianzhuang_mark
                ,obuid as obu_id
                ,'调整数据' as data_type
                ,case 
                    when price > 0 then '调增'
                    when price < 0 then '调减'
                end as adjust_type
                ,'客车' as kehuo
                ,case 
                    when month in ('2023-07','2023-08','2023-09') then '23年3季度客车工行'
                    when month in ('2023-10','2023-11','2023-12') then '23年4季度客车工行'
                end as scene2
                ,0 as shuihou_price
                ,'2023-12' as month
            from 
                bigdata.cdx_obu_yecai_jiesuanwenjian_2401_keche_gonghang_tiaozhengmingxi2
            union all 
            select 
                to_date('2023-12-31')
                ,scene
                ,'个人付费' as buyer
                ,price as price
                ,null as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type 
                ,'调减' as adjust_type
                ,'客车' as kehuo 
                ,'客车个人付费2023年调减0066户' as scene
                ,0 as shuihou_price
                ,'2023-12' as month
            from 
                bigdata.cdx_dwd_obu_income_all_bf_i_tiaozheng_gerenshouru0226_0066hu
            union all 
            select --20个业财调整在24年，大表在23年
                to_date('2024-01-31') as date
                ,'新办签约' as scene
                ,'建行-首次消费时间' as buyer
                ,-80 as price
                ,'非前装' as qianzhuang_mark
                ,obu_id
                ,'调整数据' as data_type
                ,'调减' as adjust_type
                ,'客车' as kehuo
                ,'23年4季度客车建行' as scene2
                ,-50/1.13-30/1.06 as shuihou_price
                ,'2024-01' as month
            from
                
                dwd.dwd_obu_income_all_bf_i a
            where 
                scene = '新办签约'
                and data_type = '应收数据'
                and obu_id in (
                '3701014132428216'
                ,'3701014132430371'
                ,'3701014132430492'
                ,'3701014132431037'
                ,'3701014132432512'
                ,'3701014132432871'
                ,'3701014132434678'
                ,'3701014321506859'
                ,'3701014321508267'
                ,'3701014321509760'
                ,'3701014321510084'
                ,'3701014321629931'
                ,'3701014321638978'
                ,'3701014321642430'
                ,'3701014321657239'
                ,'3701014321664461'
                ,'3701014321678078'
                ,'3701014321680966'
                ,'3701014321694655'
                ,'3701014321695566')
                and month < '2024-01'
            
            union all 
            select 
                date
                ,scene
                ,buyer
                ,price
                ,qianzhuang_mark
                ,obu_id
                ,data_type
                ,adjust_type
                ,kehuo
                ,scene2
                ,shuihou_price
                ,month
            from
                bigdata.cdx_dwd_obu_income_all_bf_i_tiaozheng_gerenshouru0411_2
        )
        where 
            substr(date,1,7) >= substr(date_add(now(),-1),1,7)
        

    '''
    #spark.sql(sql1).write.partitionBy('month').saveAsTable('dwd.dwd_obu_income_all_bf_i',mode = 'overwrite')
    spark.sql(sql1).write.mode("overwrite").insertInto('dwd.dwd_obu_income_all_bf_i')

def dwd_data_tech_obu_income_yecai_mid2():
    sql1 = '''
        select 
            *
        from 
            dwd.dwd_obu_income_all_bf_i
        where
            kehuo = '货车'
            and case
                when scene2 = '激活签约' and buyer = '建行' then 0
                when scene2 = '减免数据' then 0
                else 1 
            end = 1 

    '''
    spark.sql(sql1).write.saveAsTable('dwd.dwd_data_tech_obu_income_yecai_mid2',mode = 'overwrite')

def dws_obu_income_fin_remission_bf_i():
    sql1 = '''
        select 
            tmp1.date 
            ,tmp1.province 
            ,tmp1.buyer
            ,case 
                when tmp2.obu_id is not null then '是'
                else '否'
            end as mark
            ,count(tmp1.obu_id) as amt
        from(
            select 
                date 
                ,case
                    when substr(a.obu_id,1,2) = '44' then	'广东'
                    when substr(a.obu_id,1,2) = '45' then	'广西'
                    when substr(a.obu_id,1,2) = '13' then	'河北'
                    when substr(a.obu_id,1,2) = '37' then	'山东'
                    when substr(a.obu_id,1,2) = '15' then	'内蒙'
                    when substr(a.obu_id,1,2) = '86' then	'天津'
                    when substr(a.obu_id,1,2) = '65' then	'新疆'
                    when substr(a.obu_id,1,2) = '53' then	'云南'
                    when substr(a.obu_id,1,2) = '40' then	'广东'
                    when substr(a.obu_id,1,2) = '20' then	'广东'
                    when substr(a.obu_id,1,2) = '19' then	'广东'
                    when substr(a.obu_id,1,2) = '36' then	'江西'
                    when substr(a.obu_id,1,2) = '33' then	'浙江'
                    when substr(a.obu_id,1,2) = '51' then	'四川'
                    when substr(a.obu_id,1,2) = '22' then	'广东'
                    else '其他'
                end as province --省份
                ,buyer
                ,obu_id
            from
                dwd.dwd_obu_income_all_bf_i a 
            where 
                kehuo = '货车'
                and  scene2 = '全量obu'
            group by 
                1,2,3,4
        )tmp1 
        left join(
            select 
                obu_id
                ,buyer
            from
                dwd.dwd_obu_income_all_bf_i a 
            where 
                kehuo = '货车'
                and  scene2 = '减免数据'
            group by 
                1,2
        )tmp2 
        on tmp1.obu_id = tmp2.obu_id and tmp1.buyer = tmp2.buyer
        group by 
            1,2,3,4
    '''
    spark.sql(sql1).write.saveAsTable('dws.dws_obu_income_fin_remission_bf_i',mode = 'overwrite')
    pg_f.create_pg_inner_table(spark, 'dws.dws_obu_income_fin_remission_bf_i', 'dws.dws_obu_income_fin_remission_bf_i')

def dwd_obu_income_all_bf_f():
    sql1 = '''
        select 
            date
            ,scene
            ,buyer
            ,price
            ,qianzhuang_mark
            ,obu_id
            ,data_type
            ,adjust_type
            ,kehuo
            ,scene2
            ,shuihou_price
            ,month
            ,case 
                when kehuo = '客车' and scene <> '新办签约' then 'etc事业部'
                when kehuo = '客车' and b.lasted_obu is not null then '直播间'
                when kehuo = '客车' and c.openname like '%智能ETC%' then '智能obu'
                when kehuo = '客车' then 'etc事业部'
                when kehuo = '货车' then '数据科技事业部'
            end as department
            ,case
                when substr(a.obu_id,1,2) = '44' then	'广东'
                when substr(a.obu_id,1,2) = '45' then	'广西'
                when substr(a.obu_id,1,2) = '13' then	'河北'
                when substr(a.obu_id,1,2) = '37' then	'山东'
                when substr(a.obu_id,1,2) = '15' then	'内蒙'
                when substr(a.obu_id,1,2) = '86' then	'天津'
                when substr(a.obu_id,1,2) = '65' then	'新疆'
                when substr(a.obu_id,1,2) = '53' then	'云南'
                when substr(a.obu_id,1,2) = '40' then	'广东'
                when substr(a.obu_id,1,2) = '20' then	'广东'
                when substr(a.obu_id,1,2) = '19' then	'广东'
                when substr(a.obu_id,1,2) = '36' then	'江西'
                when substr(a.obu_id,1,2) = '33' then	'浙江'
                when substr(a.obu_id,1,2) = '51' then	'四川'
                when substr(a.obu_id,1,2) = '22' then	'广东'
                else '其他'
            end as province --省份
        from 
            dwd.dwd_obu_income_all_bf_i a 
        left join(
            select 
                lasted_obu
            from(
                select 
                    lasted_obu
                from 
                    ods.agent_order_external_channel
                group by 
                    lasted_obu
                union all  --直播间新逻辑
                select 
                    s.sn as lasted_obu
                from ods.jushuitan_order o  
                inner join ods.jushuitan_order_sns s on s.o_id = o.o_id
                inner join ods.jushuitan_order_items i on i.uuid_str = o.uuid_str
                group by 
                    1
            )
            group by 
                1
        )b 
        on a.obu_id = b.lasted_obu
        left join(
            select 
                obuid
                ,max(openname) as openname
            from 
                dwd.etc_obu 
            group by 
                1
        )c 
        on a.obu_id = c.obuid 
        
    '''
    spark.sql(sql1).write.saveAsTable('dwd.dwd_obu_income_all_bf_f',mode = 'overwrite')
    
def dws_obu_income_all_bf_f():
    sql1 = '''
        select 
            to_date(date) as date
            ,scene
            ,buyer
            ,qianzhuang_mark
            ,data_type
            ,adjust_type
            ,kehuo
            ,scene2
            ,department
            ,province
            ,cast (price as double) as price
            ,count(obu_id) obu_amt
            ,sum(price) as money
        from 
            dwd.dwd_obu_income_all_bf_f
        group by 
            1,2,3,4,5,6,7,8,9,10,11

    '''
    spark.sql(sql1).write.saveAsTable('dws.dws_obu_income_all_bf_f',mode = 'overwrite')
    pg_f.create_pg_inner_table(spark, 'dws.dws_obu_income_all_bf_f', 'dws.dws_obu_income_all_bf_f')
def dwd_etc_financial_detail_qita_i():
    sql1 = '''
        select 
            *
        from(
            --dwd.dwd_etc_financial_detail_qita_i

            --dwd.dwd_etc_financial_detail_qita_yingshou

            select 
                sale_department
                ,service_scene2
                ,obu_should_time as date 
                ,obu_id
                ,should_amount as price
                ,a.id
                ,a.ds
                ,'应收' as scene
                ,'正常数据' as data_type 
                ,substr(obu_should_time,1,7) as month
            from 
                dwd.dwd_etc_financial_detail_f a 
            where 
                ds = date_format(now(),'yyyyMMdd')
                --and service_scene2 in('2.99元取消','权益升级','换绑违约金','转产品')
                and service_scene2 not in ('新办','更换','注销','换绑退费')
                and service_type = '收款'
                and obu_should_time is not null
                and should_amount <> 0
                and should_amount is not null
                and nvl(openname,'1') not in('世纪恒通在线发行','威海商行-微信银行','威海商行-手机银行')
                and obu_kucun_type is null
            group by 
                1,2,3,4,5,6,7,8,9,10
            union all 
            --dwd.dwd_etc_financial_detail_qita_yingshou_tiaojian

            select 
                sale_department
                ,service_scene2
                ,should_refund_time
                ,obu_id
                ,0 - should_amount as price
                ,a.id
                ,a.ds
                ,'应收调减' as scene
                ,'正常数据' as data_type
                ,substr(should_refund_time,1,7) as month
            from 
                dwd.dwd_etc_financial_detail_f a 
            where 
                ds = date_format(now(),'yyyyMMdd')
                --and service_scene2 in('2.99元取消','权益升级','换绑违约金','转产品')
                and service_scene2 not in ('新办','更换','注销','换绑退费')
                and service_type = '退款'
                and should_refund_time is not null
                and should_amount <> 0
                and should_amount is not null
                and nvl(openname,'1') not in('世纪恒通在线发行','威海商行-微信银行','威海商行-手机银行')
                and obu_kucun_type is null
            group by 
                1,2,3,4,5,6,7,8,9,10


            union all 

            --dwd.dwd_etc_financial_detail_qita_shishou
            select 
                sale_department
                ,service_scene2
                ,to_date(actual_time) as actual_time
                ,obu_id
                ,actual_amount
                ,id
                ,ds
                ,'实收' as scene
                ,'正常数据' as data_type 
                ,substr(actual_time,1,7) as month
            from 
                dwd.dwd_etc_financial_detail_f
            where 
                ds = date_format(now(),'yyyyMMdd')
                --and service_scene2 in('2.99元取消','权益升级','换绑违约金','转产品')
                and service_scene2 not in ('新办','更换','注销','换绑退费')
                and service_type = '收款'
                and actual_time is not null
                and nvl(openname,'1') not in('世纪恒通在线发行','威海商行-微信银行','威海商行-手机银行')
                and obu_kucun_type is null 
            group by 
                1,2,3,4,5,6,7,8,9,10
            union all 
            --dwd.dwd_etc_financial_detail_qita_shishou_tiaojian
            select 
                sale_department
                ,service_scene2
                ,to_date(actual_time) as date
                ,obu_id
                ,0 - actual_amount as price
                ,1 as id
                ,ds
                ,'实收调减' as scene
                ,'正常数据' as data_type 
                ,substr(actual_time,1,7) as month
            from 
                dwd.dwd_etc_financial_detail_f
            where 
                ds = date_format(now(),'yyyyMMdd')
                --and service_scene2 in('2.99元取消','权益升级','换绑违约金','转产品')
                and service_scene2 not in ('新办','更换','注销','换绑退费')
                and service_type = '退款'
                and actual_time is not null
                and nvl(openname,'1') not in('世纪恒通在线发行','威海商行-微信银行','威海商行-手机银行')
                and obu_kucun_type is null 
            group by 1,2,3,4,5,6,7,8,9,10
            union all 
            select 
                *
            from 
                bigdata.cdx_dwd_etc_financial_detail_qita_i_tiaozheng202404 --24年1月2月调整在4月
        )
        where 
            substr(date,1,7) >= substr(date_add(now(),-1),1,7)
            and obu_id is not null 
            and trim(obu_id) <> ''
        
    '''
    spark.sql(sql1).write.mode('overwrite').insertInto('dwd.dwd_etc_financial_detail_qita_i')
    sql2 = '''
        select 
            data_type
            ,sale_department
            ,service_scene2
            ,scene
            ,date
            ,price
            ,count(obu_id) as obu_amt 
            ,sum(price) as money 
        from 
            dwd.dwd_etc_financial_detail_qita_i
        group by 
            1,2,3,4,5,6
    '''
    spark.sql(sql2).write.saveAsTable('dws.dws_etc_financial_detail_qita_i',mode = 'overwrite')
    pg_f.create_pg_inner_table(spark, 'dws.dws_etc_financial_detail_qita_i', 'dws.dws_etc_financial_detail_qita_i')
def dws_etc_equipment_amount_new_i():
    sql1 = '''
        select 
            substr(gmt_payment,1,7) as month
            ,'扣款' as type
            ,sum(equipment_amount) as money
        from 
            dwd.dwd_data_tech_equipment_amount_new_i
        where
            pay_state = 'SUCCESS'
            and department = 'ETC事业部' --归属事业部
            and substr(gmt_payment,1,7) >= '2024-03'
        group by 
            1
        union all 
        select 
            substr(gmt_refund,1,7) as month
            ,'退款' as type
            ,sum(equipment_amount) as money
        from 
            dwd.dwd_data_tech_equipment_amount_new_i
        where
            pay_state = 'SUCCESS'
            and department = 'ETC事业部' --归属事业部
            and substr(gmt_refund,1,7) >= '2024-03'
        group by 
            1

    '''
    spark.sql(sql1).write.saveAsTable('dws.dws_etc_equipment_amount_new_i',mode = 'overwrite')
    pg_f.create_pg_inner_table(spark, 'dws.dws_etc_equipment_amount_new_i', 'dws.dws_etc_equipment_amount_new_i')

def dwd_etc_financial_detail_qita_shouhou_zhuxiao():
    sql1 = '''
        select 
            *
            ,substr(date,1,7) as month
        from(
            select 
                '应收数据' as data_type
                ,'售后付费' as scene
                ,'应收数据' as adjust_type
                ,sale_department
                ,to_date(obu_should_time) as date
                ,obu_id
                ,should_amount as price
                ,id
                ,ds
                
            from 
                dwd.dwd_etc_financial_detail_f a

            where 
                ds = date_format(now(),'yyyyMMdd')
                and service_scene = '更换'
                and service_type = '收款'
                and obu_should_time is not null
                and should_amount <> 0
                and should_amount is not null
                and sale_department <> 'etc事业部'


            group by 
                1,2,3,4,5,6,7,8,9
            union all 
            select 
                '实收数据' as data_type
                ,'售后付费' as scene
                ,'实收数据' as adjust_type
                ,sale_department
                ,to_date(actual_time) as actual_time
                ,obu_id
                ,actual_amount
                ,id
                ,ds
            from 
                dwd.dwd_etc_financial_detail_f
            where 
                ds = date_format(now(),'yyyyMMdd')
                and service_scene = '更换'
                and service_type = '收款'
                and actual_time is not null
                and sale_department <> 'etc事业部'
                
            group by 1,2,3,4,5,6,7,8,9
            union all 
            select 
                '应收数据' as data_type
                ,'注销付费' as scene
                ,'应收数据' as adjust_type
                ,sale_department
                ,to_date(coalesce(obu_should_time,card_should_time)) as issuedtime
                ,nvl(obu_id,id) as obu_id
                ,nvl(should_amount,0) as should_amount
                ,id
                ,ds
            from 
                dwd.dwd_etc_financial_detail_f
            where 
                ds = date_format(now(),'yyyyMMdd')
                and service_scene = '注销'
                and service_type = '收款'
                and coalesce(obu_should_time,card_should_time) is not null
                and sale_department <> 'etc事业部'
                
            group by 
                1,2,3,4,5,6,7,8,9
            union all 
            select 
                '实收数据' as data_type
                ,'注销付费' as scene
                ,'实收数据' as adjust_type
                ,sale_department
                ,to_date(actual_time) as actual_time
                ,nvl(obu_id,id) as obu_id
                ,actual_amount as actual_amount
                ,id
                ,ds

            from 
                dwd.dwd_etc_financial_detail_f
            where 
                ds = date_format(now(),'yyyyMMdd')
                and service_scene = '注销'
                and service_type = '收款'
                and actual_time is not null
                and sale_department <> 'etc事业部'
                

            group by 1,2,3,4,5,6,7,8,9
            union all 
            select 

                '应收数据' as data_type
                ,'售后付费' as scene
                ,'调减' as adjust_type
                ,sale_department
                ,should_refund_time
                ,obu_id
                ,0 - should_amount as price
                ,a.id
                ,a.ds
            from 
                dwd.dwd_etc_financial_detail_f a 
            where 
                ds = date_format(now(),'yyyyMMdd')
                and service_scene2 = '更换'
                and service_type = '退款'
                and should_refund_time is not null
                and should_amount <> 0
                and should_amount is not null
                and sale_department <> 'etc事业部'
            group by 
                1,2,3,4,5,6,7,8,9
            union all 
            select 

                '应收数据' as data_type
                ,'注销付费' as scene
                ,'调减' as adjust_type
                ,sale_department
                ,should_refund_time
                ,nvl(obu_id,id) as obu_id
                ,0 - should_amount as price
                ,a.id
                ,a.ds
            from 
                dwd.dwd_etc_financial_detail_f a 
            where 
                ds = date_format(now(),'yyyyMMdd')
                and service_scene2 = '注销'
                and service_type = '退款'
                and should_refund_time is not null
                and should_amount <> 0
                and should_amount is not null
                and sale_department <> 'etc事业部'
            group by 
                1,2,3,4,5,6,7,8,9
            union all 

            select 
                '实收数据' as data_type
                ,'售后付费' as scene
                ,'调减' as adjust_type
                ,sale_department
                ,to_date(actual_time) as date
                ,obu_id
                ,0 - actual_amount as price
                ,1 as id
                ,ds
            from 
                dwd.dwd_etc_financial_detail_f
            where 
                ds = date_format(now(),'yyyyMMdd')
                and service_scene2 = '更换'
                and service_type = '退款'
                and actual_time is not null
                and sale_department <> 'etc事业部'
            group by 1,2,3,4,5,6,7,8,9
            union all 

            select 
                '实收数据' as data_type
                ,'注销付费' as scene
                ,'调减' as adjust_type
                ,sale_department
                ,to_date(actual_time) as date
                ,obu_id
                ,0 - actual_amount as price
                ,1 as id
                ,ds
            from 
                dwd.dwd_etc_financial_detail_f
            where 
                ds = date_format(now(),'yyyyMMdd')
                and service_scene2 = '注销'
                and service_type = '退款'
                and actual_time is not null
                and sale_department <> 'etc事业部'
            group by 1,2,3,4,5,6,7,8,9
        )
        where 
            substr(date,1,7) >= substr(date_add(now(),-1),1,7)
    '''
    spark.sql(sql1).write.mode("overwrite").insertInto('dwd.dwd_etc_financial_detail_qita_shouhou_zhuxiao')

    sql1 = ''' 
        select 
            data_type
            ,scene
            ,adjust_type
            ,sale_department
            ,date
            ,price
            ,count(obu_id) as obu_amt
            ,sum(price) as money
        from 
            dwd.dwd_etc_financial_detail_qita_shouhou_zhuxiao 
        group by 
            1,2,3,4,5,6
    '''
    spark.sql(sql1).write.saveAsTable('dws.dws_etc_financial_detail_qita_shouhou_zhuxiao',mode = 'overwrite')
    pg_f.create_pg_inner_table(spark, 'dws.dws_etc_financial_detail_qita_shouhou_zhuxiao', 'dws.dws_etc_financial_detail_qita_shouhou_zhuxiao')

if __name__ == '__main__':
    start_time = datetime.now()
    spark = get_or_create_spark("dwd_obu_income_all_bf_i")
    spark.conf.set("spark.sql.parquet.writeLegacyFormat", True)
    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
    spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "2000")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    pg: Engine = create_engine(
        "postgresql://bigdata:bigdata%402020@gp-bp17dozob75m1mq14-master.gpdbmaster.rds.aliyuncs.com:5432/duoduo")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dwd_obu_income_fin_bf_i()
    dwd_obu_income_etc_bf_i()
    dwd_obu_income_all_bf_i()
    dwd_data_tech_obu_income_yecai_mid2()
    dws_obu_income_fin_remission_bf_i()
    dwd_obu_income_all_bf_f()
    dws_obu_income_all_bf_f()
    dwd_etc_financial_detail_qita_i()
    dws_etc_equipment_amount_new_i()
    dwd_etc_financial_detail_qita_shouhou_zhuxiao()
    print("用时{}s".format((datetime.now()-start_time).total_seconds()))


