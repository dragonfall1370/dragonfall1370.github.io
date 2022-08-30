---##############################################
---#### This dim view contains custom values from qualtrics
---#### in case you are missing a custom value; try unpivotting it in the second CTE below
---##############################################

/*
--tried to unpivot the table using a dbt_utils; failed and had to move ahead
--feel free to finish this job
prep as (   
    select
      response_id,
      
  
    sum(
      
      case
      when key = 'Q_DataPolicyViolations'
        then value
      else 0
      end
    )
    
      
            as "Q_DataPolicyViolations"
      
    
    ,
  
    sum(
      
      case
      when key = 'Q_Language'
        then value
      else 0
      end
    )
    
      
            as "Q_Language"
      
    
    ,
  
    sum(
      
      case
      when key = 'isNew'
        then value
      else 0
      end
    )
    
      
            as "isNew"
      
    
    ,
  
    sum(
      
      case
      when key = 'LS'
        then value
      else 0
      end
    )
    
      
            as "LS"
      
    
    ,
  
    sum(
      
      case
      when key = 'Propel'
        then value
      else 0
      end
    )
    
      
            as "Propel"
      
    
    ,
  
    sum(
      
      case
      when key = 'LiquidIV'
        then value
      else 0
      end
    )
    
      
            as "LiquidIV"
      
    
    ,
  
    sum(
      
      case
      when key = 'Yeti'
        then value
      else 0
      end
    )
    
      
            as "Yeti"
      
    
    ,
  
    sum(
      
      case
      when key = 'input'
        then value
      else 0
      end
    )
    
      
            as "input"
      
    
    ,
  
    sum(
      
      case
      when key = 'Soma'
        then value
      else 0
      end
    )
    
      
            as "Soma"
      
    
    ,
  
    sum(
      
      case
      when key = 'Age Group'
        then value
      else 0
      end
    )
    
      
            as "Age Group"
      
    
    ,
  
    sum(
      
      case
      when key = 'Hydroflask'
        then value
      else 0
      end
    )
    
      
            as "Hydroflask"
      
    
    ,
  
    sum(
      
      case
      when key = 'Purchased'
        then value
      else 0
      end
    )
    
      
            as "Purchased"
      
    
    ,
  
    sum(
      
      case
      when key = 'TolunaEnc'
        then value
      else 0
      end
    )
    
      
            as "TolunaEnc"
      
    
    ,
  
    sum(
      
      case
      when key = 'Soda Stream'
        then value
      else 0
      end
    )
    
      
            as "Soda Stream"
      
    
    ,
  
    sum(
      
      case
      when key = 'Consider'
        then value
      else 0
      end
    )
    
      
            as "Consider"
      
    
    ,
  
    sum(
      
      case
      when key = 'clean'
        then value
      else 0
      end
    )
    
      
            as "clean"
      
    
    ,
  
    sum(
      
      case
      when key = 'Fiji'
        then value
      else 0
      end
    )
    
      
            as "Fiji"
      
    
    ,
  
    sum(
      
      case
      when key = 'boost'
        then value
      else 0
      end
    )
    
      
            as "boost"
      
    
    ,
  
    sum(
      
      case
      when key = 'DrinkPoppi'
        then value
      else 0
      end
    )
    
      
            as "DrinkPoppi"
      
    
    ,
  
    sum(
      
      case
      when key = 'Owala'
        then value
      else 0
      end
    )
    
      
            as "Owala"
      
    
    ,
  
    sum(
      
      case
      when key = 'RISN'
        then value
      else 0
      end
    )
    
      
            as "RISN"
      
    
    ,
  
    sum(
      
      case
      when key = 'gc'
        then value
      else 0
      end
    )
    
      
            as "gc"
      
    
    ,
  
    sum(
      
      case
      when key = 'Country'
        then value
      else 0
      end
    )
    
      
            as "Country"
      
    
    ,
  
    sum(
      
      case
      when key = 'sname'
        then value
      else 0
      end
    )
    
      
            as "sname"
      
    
    ,
  
    sum(
      
      case
      when key = 'USRegion'
        then value
      else 0
      end
    )
    
      
            as "USRegion"
      
    
    ,
  
    sum(
      
      case
      when key = 'launch'
        then value
      else 0
      end
    )
    
      
            as "launch"
      
    
    ,
  
    sum(
      
      case
      when key = 'Corkcircle'
        then value
      else 0
      end
    )
    
      
            as "Corkcircle"
      
    
    ,
  
    sum(
      
      case
      when key = 'Bubly'
        then value
      else 0
      end
    )
    
      
            as "Bubly"
      
    
    ,
  
    sum(
      
      case
      when key = 'GateradeGX'
        then value
      else 0
      end
    )
    
      
            as "GateradeGX"
      
    
    ,
  
    sum(
      
      case
      when key = 'Q_TotalDuration'
        then value
      else 0
      end
    )
    
      
            as "Q_TotalDuration"
      
    
    ,
  
    sum(
      
      case
      when key = 'Buy'
        then value
      else 0
      end
    )
    
      
            as "Buy"
      
    
    ,
  
    sum(
      
      case
      when key = 'Swell'
        then value
      else 0
      end
    )
    
      
            as "Swell"
      
    
    ,
  
    sum(
      
      case
      when key = 'Gathered'
        then value
      else 0
      end
    )
    
      
            as "Gathered"
      
    
    ,
  
    sum(
      
      case
      when key = 'SurveyVersionID'
        then value
      else 0
      end
    )
    
      
            as "SurveyVersionID"
      
    
    ,
  
    sum(
      
      case
      when key = 'term'
        then value
      else 0
      end
    )
    
      
            as "term"
      
    
    ,
  
    sum(
      
      case
      when key = 'Region'
        then value
      else 0
      end
    )
    
      
            as "Region"
      
    
    ,
  
    sum(
      
      case
      when key = 'test'
        then value
      else 0
      end
    )
    
      
            as "test"
      
    
    ,
  
    sum(
      
      case
      when key = 'Spindrift'
        then value
      else 0
      end
    )
    
      
            as "Spindrift"
      
    
    ,
  
    sum(
      
      case
      when key = 'Szent'
        then value
      else 0
      end
    )
    
      
            as "Szent"
      
    
    ,
  
    sum(
      
      case
      when key = 'wave'
        then value
      else 0
      end
    )
    
      
            as "wave"
      
    
    ,
  
    sum(
      
      case
      when key = 'Perrier'
        then value
      else 0
      end
    )
    
      
            as "Perrier"
      
    
    ,
  
    sum(
      
      case
      when key = 'Pepsi'
        then value
      else 0
      end
    )
    
      
            as "Pepsi"
      
    
    ,
  
    sum(
      
      case
      when key = 'Bai'
        then value
      else 0
      end
    )
    
      
            as "Bai"
      
    
    ,
  
    sum(
      
      case
      when key = 'ResponseID'
        then value
      else 0
      end
    )
    
      
            as "ResponseID"
      
    
    ,
  
    sum(
      
      case
      when key = 'Other'
        then value
      else 0
      end
    )
    
      
            as "Other"
      
    
    ,
  
    sum(
      
      case
      when key = 'V'
        then value
      else 0
      end
    )
    
      
            as "V"
      
    
    ,
  
    sum(
      
      case
      when key = 'La Croix'
        then value
      else 0
      end
    )
    
      
            as "La Croix"
      
    
    ,
  
    sum(
      
      case
      when key = 'Cirkul'
        then value
      else 0
      end
    )
    
      
            as "Cirkul"
      
    
    ,
  
    sum(
      
      case
      when key = 'Larq'
        then value
      else 0
      end
    )
    
      
            as "Larq"
      
    
    ,
  
    sum(
      
      case
      when key = 'Mio'
        then value
      else 0
      end
    )
    
      
            as "Mio"
      
    
    ,
  
    sum(
      
      case
      when key = 'USPriorityBrands'
        then value
      else 0
      end
    )
    
      
            as "USPriorityBrands"
      
    
    ,
  
    sum(
      
      case
      when key = 'Liquid Death'
        then value
      else 0
      end
    )
    
      
            as "Liquid Death"
      
    
    ,
  
    sum(
      
      case
      when key = 'gid'
        then value
      else 0
      end
    )
    
      
            as "gid"
      
    
    ,
  
    sum(
      
      case
      when key = 'Nalgene'
        then value
      else 0
      end
    )
    
      
            as "Nalgene"
      
    
    ,
  
    sum(
      
      case
      when key = 'Waterdrop'
        then value
      else 0
      end
    )
    
      
            as "Waterdrop"
      
    
    ,
  
    sum(
      
      case
      when key = 'air up'
        then value
      else 0
      end
    )
    
      
            as "air up"
      
    
    ,
  
    sum(
      
      case
      when key = 'opp'
        then value
      else 0
      end
    )
    
      
            as "opp"
      
    
    ,
  
    sum(
      
      case
      when key = 'Aware'
        then value
      else 0
      end
    )
    
      
            as "Aware"
      
    
    ,
  
    sum(
      
      case
      when key = 'rid'
        then value
      else 0
      end
    )
    
      
            as "rid"
      
    
    ,
  
    sum(
      
      case
      when key = 'cintid'
        then value
      else 0
      end
    )
    
      
            as "cintid"
      
    
    ,
  
    sum(
      
      case
      when key = 'Smartwater'
        then value
      else 0
      end
    )
    
      
            as "Smartwater"
      
    
    ,
  
    sum(
      
      case
      when key = 'Q13'
        then value
      else 0
      end
    )
    
      
            as "Q13"
      
    
    ,
  
    sum(
      
      case
      when key = 'Q14a'
        then value
      else 0
      end
    )
    
      
            as "Q14a"
      
    
    ,
  
    sum(
      
      case
      when key = 'customer_type'
        then value
      else 0
      end
    )
    
      
            as "customer_type"
      
    
    ,
  
    sum(
      
      case
      when key = 'shop'
        then value
      else 0
      end
    )
    
      
            as "shop"
      
    
    ,
  
    sum(
      
      case
      when key = 'shopLocale'
        then value
      else 0
      end
    )
    
      
            as "shopLocale"
      
    
    ,
  
    sum(
      
      case
      when key = 'order_id'
        then value
      else 0
      end
    )
    
      
            as "order_id"
      
    
    ,
  
    sum(
      
      case
      when key = 'tp'
        then value
      else 0
      end
    )
    
      
            as "tp"
      
    
    ,
  
    sum(
      
      case
      when key = 'customer_id'
        then value
      else 0
      end
    )
    
      
            as "customer_id"
      
    
    ,
  
    sum(
      
      case
      when key = 'Group'
        then value
      else 0
      end
    )
    
      
            as "Group"
      
    
    ,
  
    sum(
      
      case
      when key = 'Customer_id'
        then value
      else 0
      end
    )
    
      
            as "Customer_id"
      
    
    ,
  
    sum(
      
      case
      when key = 'Co'
        then value
      else 0
      end
    )
    
      
            as "Co"
      
    
    ,
  
    sum(
      
      case
      when key = 'ID'
        then value
      else 0
      end
    )
    
      
            as "ID"
      
    
    ,
  
    sum(
      
      case
      when key = 'Scr'
        then value
      else 0
      end
    )
    
      
            as "Scr"
      
    
    ,
  
    sum(
      
      case
      when key = 'Q1pipe'
        then value
      else 0
      end
    )
    
      
            as "Q1pipe"
      
    
    ,
  
    sum(
      
      case
      when key = 'TP'
        then value
      else 0
      end
    )
    
      
            as "TP"
      
    
    ,
  
    sum(
      
      case
      when key = 'Hero'
        then value
      else 0
      end
    )
    
      
            as "Hero"
      
    
    ,
  
    sum(
      
      case
      when key = 'SC0'
        then value
      else 0
      end
    )
    
      
            as "SC0"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob3R'
        then value
      else 0
      end
    )
    
      
            as "Prob3R"
      
    
    ,
  
    sum(
      
      case
      when key = 'SEGA'
        then value
      else 0
      end
    )
    
      
            as "SEGA"
      
    
    ,
  
    sum(
      
      case
      when key = 'V30'
        then value
      else 0
      end
    )
    
      
            as "V30"
      
    
    ,
  
    sum(
      
      case
      when key = 'SEGE'
        then value
      else 0
      end
    )
    
      
            as "SEGE"
      
    
    ,
  
    sum(
      
      case
      when key = 'V31'
        then value
      else 0
      end
    )
    
      
            as "V31"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob1'
        then value
      else 0
      end
    )
    
      
            as "Prob1"
      
    
    ,
  
    sum(
      
      case
      when key = 'V37'
        then value
      else 0
      end
    )
    
      
            as "V37"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob3'
        then value
      else 0
      end
    )
    
      
            as "Prob3"
      
    
    ,
  
    sum(
      
      case
      when key = 'V42'
        then value
      else 0
      end
    )
    
      
            as "V42"
      
    
    ,
  
    sum(
      
      case
      when key = 'SUM'
        then value
      else 0
      end
    )
    
      
            as "SUM"
      
    
    ,
  
    sum(
      
      case
      when key = 'V44'
        then value
      else 0
      end
    )
    
      
            as "V44"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob5R'
        then value
      else 0
      end
    )
    
      
            as "Prob5R"
      
    
    ,
  
    sum(
      
      case
      when key = 'V50'
        then value
      else 0
      end
    )
    
      
            as "V50"
      
    
    ,
  
    sum(
      
      case
      when key = 'E3'
        then value
      else 0
      end
    )
    
      
            as "E3"
      
    
    ,
  
    sum(
      
      case
      when key = 'V51'
        then value
      else 0
      end
    )
    
      
            as "V51"
      
    
    ,
  
    sum(
      
      case
      when key = 'V33'
        then value
      else 0
      end
    )
    
      
            as "V33"
      
    
    ,
  
    sum(
      
      case
      when key = 'V26'
        then value
      else 0
      end
    )
    
      
            as "V26"
      
    
    ,
  
    sum(
      
      case
      when key = 'V34'
        then value
      else 0
      end
    )
    
      
            as "V34"
      
    
    ,
  
    sum(
      
      case
      when key = 'Max'
        then value
      else 0
      end
    )
    
      
            as "Max"
      
    
    ,
  
    sum(
      
      case
      when key = 'V38'
        then value
      else 0
      end
    )
    
      
            as "V38"
      
    
    ,
  
    sum(
      
      case
      when key = 'SEGB'
        then value
      else 0
      end
    )
    
      
            as "SEGB"
      
    
    ,
  
    sum(
      
      case
      when key = 'V39'
        then value
      else 0
      end
    )
    
      
            as "V39"
      
    
    ,
  
    sum(
      
      case
      when key = 'SEGC'
        then value
      else 0
      end
    )
    
      
            as "SEGC"
      
    
    ,
  
    sum(
      
      case
      when key = 'V45'
        then value
      else 0
      end
    )
    
      
            as "V45"
      
    
    ,
  
    sum(
      
      case
      when key = 'SEGD'
        then value
      else 0
      end
    )
    
      
            as "SEGD"
      
    
    ,
  
    sum(
      
      case
      when key = 'V49'
        then value
      else 0
      end
    )
    
      
            as "V49"
      
    
    ,
  
    sum(
      
      case
      when key = 'Seg_n'
        then value
      else 0
      end
    )
    
      
            as "Seg_n"
      
    
    ,
  
    sum(
      
      case
      when key = 'V16'
        then value
      else 0
      end
    )
    
      
            as "V16"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob2'
        then value
      else 0
      end
    )
    
      
            as "Prob2"
      
    
    ,
  
    sum(
      
      case
      when key = 'V18'
        then value
      else 0
      end
    )
    
      
            as "V18"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob5'
        then value
      else 0
      end
    )
    
      
            as "Prob5"
      
    
    ,
  
    sum(
      
      case
      when key = 'V19'
        then value
      else 0
      end
    )
    
      
            as "V19"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob2R'
        then value
      else 0
      end
    )
    
      
            as "Prob2R"
      
    
    ,
  
    sum(
      
      case
      when key = 'V20'
        then value
      else 0
      end
    )
    
      
            as "V20"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob4R'
        then value
      else 0
      end
    )
    
      
            as "Prob4R"
      
    
    ,
  
    sum(
      
      case
      when key = 'V21'
        then value
      else 0
      end
    )
    
      
            as "V21"
      
    
    ,
  
    sum(
      
      case
      when key = 'Segment'
        then value
      else 0
      end
    )
    
      
            as "Segment"
      
    
    ,
  
    sum(
      
      case
      when key = 'V24'
        then value
      else 0
      end
    )
    
      
            as "V24"
      
    
    ,
  
    sum(
      
      case
      when key = 'E1'
        then value
      else 0
      end
    )
    
      
            as "E1"
      
    
    ,
  
    sum(
      
      case
      when key = 'V27'
        then value
      else 0
      end
    )
    
      
            as "V27"
      
    
    ,
  
    sum(
      
      case
      when key = 'E4'
        then value
      else 0
      end
    )
    
      
            as "E4"
      
    
    ,
  
    sum(
      
      case
      when key = 'V28'
        then value
      else 0
      end
    )
    
      
            as "V28"
      
    
    ,
  
    sum(
      
      case
      when key = 'V32'
        then value
      else 0
      end
    )
    
      
            as "V32"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob4'
        then value
      else 0
      end
    )
    
      
            as "Prob4"
      
    
    ,
  
    sum(
      
      case
      when key = 'V35'
        then value
      else 0
      end
    )
    
      
            as "V35"
      
    
    ,
  
    sum(
      
      case
      when key = 'Prob1R'
        then value
      else 0
      end
    )
    
      
            as "Prob1R"
      
    
    ,
  
    sum(
      
      case
      when key = 'V40'
        then value
      else 0
      end
    )
    
      
            as "V40"
      
    
    ,
  
    sum(
      
      case
      when key = 'E2'
        then value
      else 0
      end
    )
    
      
            as "E2"
      
    
    ,
  
    sum(
      
      case
      when key = 'V41'
        then value
      else 0
      end
    )
    
      
            as "V41"
      
    
    ,
  
    sum(
      
      case
      when key = 'E5'
        then value
      else 0
      end
    )
    
      
            as "E5"
      
    
    ,
  
    sum(
      
      case
      when key = 'V43'
        then value
      else 0
      end
    )
    
      
            as "V43"
      
    
    ,
  
    sum(
      
      case
      when key = 'V36'
        then value
      else 0
      end
    )
    
      
            as "V36"
      
    
    ,
  
    sum(
      
      case
      when key = 'V46'
        then value
      else 0
      end
    )
    
      
            as "V46"
      
    
    ,
  
    sum(
      
      case
      when key = 'V52'
        then value
      else 0
      end
    )
    
      
            as "V52"
      
    
    ,
  
    sum(
      
      case
      when key = 'V47'
        then value
      else 0
      end
    )
    
      
            as "V47"
      
    
    ,
  
    sum(
      
      case
      when key = 'V17'
        then value
      else 0
      end
    )
    
      
            as "V17"
      
    
    ,
  
    sum(
      
      case
      when key = 'V48'
        then value
      else 0
      end
    )
    
      
            as "V48"
      
    
    ,
  
    sum(
      
      case
      when key = 'V22'
        then value
      else 0
      end
    )
    
      
            as "V22"
      
    
    ,
  
    sum(
      
      case
      when key = 'V15'
        then value
      else 0
      end
    )
    
      
            as "V15"
      
    
    ,
  
    sum(
      
      case
      when key = 'V25'
        then value
      else 0
      end
    )
    
      
            as "V25"
      
    
    ,
  
    sum(
      
      case
      when key = 'V23'
        then value
      else 0
      end
    )
    
      
            as "V23"
      
    
    ,
  
    sum(
      
      case
      when key = 'V29'
        then value
      else 0
      end
    )
    
      
            as "V29"
      
    
    ,
  
    sum(
      
      case
      when key = 'V2'
        then value
      else 0
      end
    )
    
      
            as "V2"
      
    
    ,
  
    sum(
      
      case
      when key = 'V6'
        then value
      else 0
      end
    )
    
      
            as "V6"
      
    
    ,
  
    sum(
      
      case
      when key = 'V3'
        then value
      else 0
      end
    )
    
      
            as "V3"
      
    
    ,
  
    sum(
      
      case
      when key = 'V7'
        then value
      else 0
      end
    )
    
      
            as "V7"
      
    
    ,
  
    sum(
      
      case
      when key = 'V4'
        then value
      else 0
      end
    )
    
      
            as "V4"
      
    
    ,
  
    sum(
      
      case
      when key = 'V9'
        then value
      else 0
      end
    )
    
      
            as "V9"
      
    
    ,
  
    sum(
      
      case
      when key = 'V11'
        then value
      else 0
      end
    )
    
      
            as "V11"
      
    
    ,
  
    sum(
      
      case
      when key = 'V10'
        then value
      else 0
      end
    )
    
      
            as "V10"
      
    
    ,
  
    sum(
      
      case
      when key = 'V14'
        then value
      else 0
      end
    )
    
      
            as "V14"
      
    
    ,
  
    sum(
      
      case
      when key = 'V1'
        then value
      else 0
      end
    )
    
      
            as "V1"
      
    
    ,
  
    sum(
      
      case
      when key = 'V13'
        then value
      else 0
      end
    )
    
      
            as "V13"
      
    
    ,
  
    sum(
      
      case
      when key = 'V5'
        then value
      else 0
      end
    )
    
      
            as "V5"
      
    
    ,
  
    sum(
      
      case
      when key = 'V8'
        then value
      else 0
      end
    )
    
      
            as "V8"
      
    
    ,
  
    sum(
      
      case
      when key = 'V12'
        then value
      else 0
      end
    )
    
      
            as "V12"
      
    
    ,
  
    sum(
      
      case
      when key = 'E'
        then value
      else 0
      end
    )
    
      
            as "E"
      
    
    ,
  
    sum(
      
      case
      when key = 'Create New Field or Choose From Dropdown...'
        then value
      else 0
      end
    )
    
      
            as "Create New Field or Choose From Dropdown..."
      
    
    ,
  
    sum(
      
      case
      when key = 'quest'
        then value
      else 0
      end
    )
    
      
            as "quest"
      
    
    
  

    from cleaning 
    group by response_id
),
*/



with

  -- pulling all the data in narrow format
  prep as (   
      select distinct
        response_id,
        key,
        value
      from
        "airup_eu_dwh"."qualtrics"."dim_survey_embedded_data"
  ),

  prep2 as (
      select distinct response_id from prep
  ),

  -- unpivotting the needed colums via self joins
  unpivotting as (
    select
      prep2.response_id,
      customer_id.value as customer_id,
      order_id.value as order_id,
      shopLocale.value as shopLocale,
      tp.value as tp,
      shop.value as shop,
      customer_type.value as customer_type
    from 
      prep2
      left join prep customer_id on prep2.response_id = customer_id.response_id and customer_id.key = 'Customer_id'
      left join prep order_id on prep2.response_id = order_id.response_id and order_id.key = 'order_id'
      left join prep shopLocale on prep2.response_id = shopLocale.response_id and shopLocale.key = 'shopLocale'
      left join prep tp on prep2.response_id = tp.response_id and tp.key = 'tp'
      left join prep shop on prep2.response_id = shop.response_id and shop.key = 'shop'
      left join prep customer_type on prep2.response_id = customer_type.response_id and customer_type.key = 'customer_type'
  )

select 
  *
from 
  unpivotting