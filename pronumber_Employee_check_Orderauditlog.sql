with New_Pronum_logic -- Temp for testing
             (Loadnum, Activity, ActivityDateTime, ActivityCount,Employee,ReasonCode,IsAutomated,LoadBasedActivity,SourceAsOfTS) as 

(SELECT DISTINCT 
            TCA.KeyColumnValue LoadNum
            ,'PRONUM ENTRIES' Activity
            ,TCA.AuditDatetime ActivityDateTime
            ,1 ActivityCount
            ,Upper(Trim(TCA.AuditUserId)) Employee
            ,Upper(Trim(TCA.AuditType)) ReasonCode
            ,(case  when TCA.AUDITCOLUMNNEWVALUE is null then 0 -- at times users remove the order number leaving the field blank so if new value is null its manual activity. 
                    when Upper(Trim(OA.ENTEREDBYPARTYCODE)) is null then 1 else 0 end) as IsAutomated -- when order audit as a manual entry is not found then automate
           
            ,TRUE LoadBasedActivity  -- even though I am joining it on Order audit log its still a load based activity because we are usig the TableColumnAudit table as a base. 
            ,GREATEST
              (
                  IFNULL(LOAD_CTL.SourceAsOfTS, to_timestamp('1970-01-01 00:00:00')),
                  IFNULL(TCA.HVR_CAPTURE_DATE, to_timestamp('1970-01-01 00:00:00'))
               ) as SourceAsOfTS
               
        FROM
            OPERATIONS_DOMAIN.DBO.V_HVR_DBO_TABLECOLUMNAUDIT TCA
            INNER JOIN OPERATIONS_DOMAIN.DBO.STG_SHIPMENT_ACTIVITY_LOADLIST LOAD_CTL 
          
            LEFT  JOIN  ORION_RAP.OA.ORDERAUDIT OA on  OA.entityID = TCA.KeyColumnValue -- matching load num
                                                   AND OA.NewValue = TCA.AuditColumnNewValue -- maching actual pronum as a new value
                                                   AND Upper(Trim(OA.ENTEREDBYPARTYCODE))=Upper(Trim(TCA.AuditUserId)) --matching entred by
                                                   AND OA.EntityTypeID = 2 -- 2 = loadnum 1 = ordernum
                                                   AND OA.ACTIONITEMTYPEID  in (5,31) --- 5 = addrefnum -- 31 Update refnum
                                                   AND OA.FIELDNAME = 'Value'
                                                   AND OA.SOURCESYSTEMID in (479,3956) -- 479 =manual entry and 3956 = execution(done in load)
                                                   AND OA.FIELDPARENT like ('T%')
                                                   
                        
        WHERE 
                TCA.TableName = 'LOADBOOKS'
            AND TCA.AuditColumnName = 'CARRIERPRONUMBER'
            AND TCA.AuditDateTime >= '2021-05-01'--  for Testing 
            AND TCA.AuditDateTime < '2021-05-07'--  For Testing 
)



,Shipment_Activity --- Creating a full set of shipment activity table for testing 
(
            Loadnum
            ,Activity
            ,ActivityDateTime
            ,ActivityDate
           -- ,ActivityDateInt
            ,ActivityHour
            ,ActivityCount
            ,Employee
            ,ReasonCode
           -- ,OrderSourceSystemId
          --  ,OrderSourceSystemName
            ,IsAutomated
            ,IsBoT
        --    ,ResolveNoBill
            ,CustomerId
            ,CustomerCode
            ,Mode
          --  ,ModeRollup
            ,BranchId
            ,BranchCode
            ,EmployeeBranchId
            ,EmployeeBranchCode
            ,EmployeePosition
           -- ,EmployeePositionRollup
           -- ,ActivityQuarter
          --  ,IsManualActivity
            --,ActivityTimeDuration
            --,ActivityClass
            ,SourceAsOfTS
        ) AS
        (
            SELECT 
                STG.LOADNUM AS LOADNUM
                ,STG.ACTIVITY AS ACTIVITY
                ,STG.ACTIVITYDATETIME AS ACTIVITYDATETIME
                ,DATE(STG.ACTIVITYDATETIME) AS ACTIVITYDATE
                --,TOINTDATE(STG.ACTIVITYDATETIME) AS ACTIVITYDATEINT
                ,HOUR(STG.ACTIVITYDATETIME) AS ACTIVITYHOUR
                ,STG.ACTIVITYCOUNT AS ACTIVITYCOUNT
                ,STG.EMPLOYEE AS EMPLOYEE
                ,STG.REASONCODE AS REASONCODE
                 ,CASE
                    WHEN STG.ISAUTOMATED IS NOT NULL THEN STG.ISAUTOMATED
                    WHEN (BOT.AUTOMATED = 'AUTO'
                    OR E.EMPCODE IS NULL
                    OR E.BRANCHCODE = '7650') THEN TRUE
                    ELSE FALSE
                 END AS ISAUTOMATED
                ,IFF(BOT.AUTOMATED = 'AUTO' ,TRUE ,FALSE) AS ISBOT
          
          
             --   ,STG.ORDERSOURCESYSTEMID AS ORDERSOURCESYSTEMID
             --  ,STG.ORDERSOURCESYSTEMNAME AS ORDERSOURCESYSTEMNAME
                ,CUSTOMER.PARTYID AS CUSTOMERID
                ,UPPER(TRIM(LOADMATCH.BILLTOCOMPCODE)) AS CUSTOMERCODE
                ,UPPER(TRIM(EXECUTIONMODE.MODENAMESIMPLE)) AS MODE
                ,CUSTOMERBRANCH.PARTYID AS BRANCHID
                ,LOADMATCH.CUSTOMERBRANCH AS BRANCHCODE
                ,EMPBRANCH.PARTYID AS EMPLOYEEBRANCHID
                ,EL.BRANCHCODE AS EMPLOYEEBRANCHCODE
                ,UPPER(TRIM(EL.PSROLE)) AS EMPLOYEEPOSITION
               
               -- ,STG.RESOLVENOBILL AS RESOLVENOBILL
                ,STG.SourceAsOfTS AS SourceAsOfTS
            FROM
                 New_Pronum_logic as STG    ---  OPERATIONS_DOMAIN.DBO.TEMP_LOAD_ORDER_ACTIVITY STG   -- for testing I am just joining NewPronum Logic table from above. 
                LEFT OUTER JOIN EXPRESS_RAP.DBO.LOADS LOADS ON
                    STG.LOADNUM = LOADS.LOADNUM
                LEFT OUTER JOIN EXPRESS_RAP.DBO.LOADMATCH LOADMATCH ON
                    LOADMATCH.LOADNUM = LOADS.LOADNUM
                LEFT OUTER JOIN MDM_RAP.MDM.PARTY CUSTOMER ON
                    LOADMATCH.BILLTOCOMPCODE = CUSTOMER.PARTYCODE
                    AND CUSTOMER.PARTYTYPEID = 3 /* Customer */
                LEFT OUTER JOIN MDM_RAP.MDM.PARTY CUSTOMERBRANCH ON
                    LOADMATCH.CUSTOMERBRANCH = CUSTOMERBRANCH.PARTYCODE
                    AND CUSTOMERBRANCH.PARTYTYPEID = 1 /* Branch */
                LEFT OUTER JOIN EXPRESS_RAP.DBO.MODE EXECUTIONMODE  ON
                    UPPER(TRIM(EXECUTIONMODE.MODE)) = UPPER(TRIM(LOADS.MODE))
                LEFT OUTER JOIN EXPRESS_RAP.DBO.EMPLOYEES E  ON
                    STG.EMPLOYEE = TRIM(E.EMPCODE)
                LEFT OUTER JOIN TRUCKLOAD_DOMAIN.DBO.DIM_EMPLOYEE_HISTORY EL ON
                    TRIM(EL.EMPLOYEECODE) = TRIM(E.EMPCODE)
                    AND STG.ACTIVITYDATETIME >= EL.ACTIVESTARTDATETIME
                    AND STG.ACTIVITYDATETIME <= EL.ACTIVEENDDATETIME
                LEFT JOIN MDM_RAP.MDM.PARTY EMPBRANCH ON
                    TRIM(EL.BRANCHCODE) = EMPBRANCH.PARTYCODE
                    AND EMPBRANCH.PARTYTYPEID = 1 /*BRANCH*/
                LEFT JOIN OPERATIONS_DOMAIN.DBO.OPEX_BOTS BOT ON
                    UPPER(TRIM(STG.EMPLOYEE)) = BOT.EMPCODE
            WHERE
                STG.LoadBasedActivity = TRUE

)


SELECT -- output
a.Activity
,a.Mode
,a.activityDate
,IsAutomated
,Sum(a.activitycount) as Act_ct

 FROM Shipment_Activity a


GROUP BY
a.Activity
,a.Mode
,a.activityDate
,IsAutomated







//    
//SELECT
//         a.employee
//         ,er.subrollupfamily
//         ,ba.branchname
//         ,a.moderollup
//         ,a.activity 
//         ,tc.taskorder 
//         ,trim(upper(ba.branchsubregion1)) as BranchRegion
//         ,count(distinct a.loadnum) as loads
//         ,sum(a.activitycount) as ct
//         ,sum(a.activitycycletimeduration)/60 as hrs
//         ,Sum(case when NP.IsautomatedA =0 then a.activitycycletimeduration/60 else 0 end) as New_Logic_Hrs
//         ,sum(NP.IsautomatedA) as New_Isautomated
//FROM Shipment_activity as a
//LEFT JOIN  New_Pronum_logic as NP  on a.loadnum = NP.Loadnum and a.employee = NP.employee and A.ActivityDatetime = NP.ActivityDatetime
//
//LEFT JOIN TRUCKLOAD_DOMAIN.DBO.V_DIM_BRANCH as ba on ba.branchID = a.employeebranchID 
//LEFT JOIN TRUCKLOAD_DOMAIN.DBO.V_DIM_CUSTOMER as c on c.customerid = a.customerid
//LEFT JOIN OPERATIONS_DOMAIN.DBO.DIM_TASK_CLASSIFICATION as tc on tc.task = a.activity
//LEFT JOIN  OPERATIONS_DOMAIN.DBO.DIM_EMPLOYEE_ROLE as er on trim(upper(a.employeeposition)) = trim(upper(er.psrole))
//WHERE 
//
// ba.primarybusinesslineid = 62
// 
//GROUP BY        
//         a.employee
//         ,er.subrollupfamily
//         ,ba.branchname
//         ,trim(upper(ba.branchsubregion1))
//         ,a.moderollup
//         ,a.activity 
//         ,tc.taskorder 
   
 

  
  
  

 
  
