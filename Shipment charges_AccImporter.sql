 
       ----INSERT INTO OPERATIONS_DOMAIN.DBO.TEMP_LOAD_ORDER_ACTIVITY  ----taking this line out to test code-- will be part of final submittal. 
                        

 
 With ORDER_LOAD_SHIPMENT_CHARGE AS -------  Running shipment charges Query from order audit log (no Change)
        (
            SELECT
                 orderauditdata.entityid ,
                 orderauditdata.entitytypeid,
                 'SHIPMENT CHARGES' Activity ,
                 TIME_SLICE(orderauditdata.EnteredDate, 30, 'SECOND') ActivityDateTime,
                 1 ActivityCount ,
                 Upper(TRIM(orderauditdata.enteredbypartycode)) Employee,
                 NULL ReasonCode ,
                 orderauditdata.sourcesystemid ,
                 Upper(TRIM(orderauditdata.sourcesystem)) sourcesystem ,
                 CASE WHEN orderauditdata.sourcesystemid in (481,480,5326,5258,2209,4226,5391,2210,4968,5140,4791,5191,5468,4757)
                 THEN TRUE
                 ELSE FALSE END IsAutomated,
                 orderauditdata.EnteredDate AS SourceAsOfTS
            FROM
                ORION_RAP.OA.ORDERAUDIT ORDERAUDITDATA 
            WHERE
                orderauditdata.entitytypeid IN (1,2)
                AND orderauditdata.categoryid = 7
                AND orderauditdata.actionitemtypeid in (21,22)
                AND orderauditdata.enteredbypartycode not in ('Orders', 'System')
                AND orderauditdata.sourcesystemid = 479
                AND orderauditdata.fieldname = 'Amount'
                and orderauditdata.entereddate >=  '2021-04-01' -- for testing
                and orderauditdata.entereddate < '2021-05-01'  -- for tesitng
        )
        
 ,SHIPMENT_CHARGES_Accesorial_Importer_addon as ----  Running Accessorial Importer query from order audit log
        (
         SELECT		
          a.ENTEREDBYPARTYCODE	
         ,TIME_SLICE(a.EnteredDate, 30, 'SECOND') as EnteredDate30
         ,1 as AccUpvol	
  
         FROM ORION_RAP.OA.ORDERAUDIT  a
               
         WHERE
             a.categoryid = 7 -- 7 =financial revision
         and a.actionitemtypeid  in (21,22) -- 21= manual rate charge -- 22 =manual rate cost 
         and a.sourcesystemid ='479'-- manual entry
         and a.fieldname ='Source System'
         and a.NewValue = 'Acc Uploader'

----For Testing  
         and a.entereddate >=  '2021-04-01'
         and a.entereddate < '2021-05-01'  
       
          
         GROUP BY
         a.ENTEREDBYPARTYCODE	
         ,TIME_SLICE(a.EnteredDate, 30, 'SECOND')
        )
        
  ,Base_ShipmentChargesActivity                            
      (OrderNum ,Loadnum, Activity, ActivityDateTime, ActivityCount, Employee, ReasonCode, OrderSourceSystemId, OrderSourceSystemName, IsAutomated , LoadBasedActivity, SourceAsOfTS) AS
    (
        SELECT DISTINCT -------------- Running the data from the Base Shipment charge query and adding acc importer addon
             ORDER_CTL.Ordernum
            ,ORDER_CTL.Loadnum
            ,ORDER_LOAD_SHIPMENT_CHARGE.Activity
            ,ORDER_LOAD_SHIPMENT_CHARGE.ActivityDateTime
            ,ORDER_LOAD_SHIPMENT_CHARGE.ActivityCount
            ,ORDER_LOAD_SHIPMENT_CHARGE.Employee
            ,ORDER_LOAD_SHIPMENT_CHARGE.ReasonCode
            ,ORDER_LOAD_SHIPMENT_CHARGE.sourcesystemid
            ,ORDER_LOAD_SHIPMENT_CHARGE.sourcesystem
            ,Case when ORDER_LOAD_SHIPMENT_CHARGE.IsAutomated = FALSE and AI.AccUpvol >=1 then TRUE else FALSE end -- Modyfing base isautomated field to include acc importer addon. 
            ,False LoadBasedActivity
            ,ORDER_LOAD_SHIPMENT_CHARGE.SourceAsOfTS
        FROM
            ORDER_LOAD_SHIPMENT_CHARGE
             INNER JOIN OPERATIONS_DOMAIN.DBO.STG_SHIPMENT_ACTIVITY_ORDERLIST ORDER_CTL
                ON  ORDER_CTL.OrderNum = ORDER_LOAD_SHIPMENT_CHARGE.entityid
                
             LEFT JOIN SHIPMENT_CHARGES_Accesorial_Importer_addon AI
                ON  AI.Entereddate30 = ORDER_LOAD_SHIPMENT_CHARGE.ActivityDateTime  -- Join Activity date time
                         and AI.ENTEREDBYPARTYCODE = ORDER_LOAD_SHIPMENT_CHARGE.Employee   -- Join Employee     
      
        WHERE
            ENTITYTYPEID = 1
            
        UNION ALL
        
        
        SELECT DISTINCT
            Null
            ,LOAD_CTL.LoadNum
            ,ORDER_LOAD_SHIPMENT_CHARGE.Activity
            ,ORDER_LOAD_SHIPMENT_CHARGE.ActivityDateTime
            ,ORDER_LOAD_SHIPMENT_CHARGE.ActivityCount
            ,ORDER_LOAD_SHIPMENT_CHARGE.Employee
            ,ORDER_LOAD_SHIPMENT_CHARGE.ReasonCode
            ,ORDER_LOAD_SHIPMENT_CHARGE.sourcesystemid
            ,ORDER_LOAD_SHIPMENT_CHARGE.sourcesystem
            ,Case when ORDER_LOAD_SHIPMENT_CHARGE.IsAutomated = FALSE and AI.AccUpvol >=1 then TRUE else FALSE end  -- Modyfing base isautomated field to include acc importer addon. 
            ,True LoadBasedActivity
            ,ORDER_LOAD_SHIPMENT_CHARGE.SourceAsOfTS
        FROM
            ORDER_LOAD_SHIPMENT_CHARGE
            INNER JOIN OPERATIONS_DOMAIN.DBO.STG_SHIPMENT_ACTIVITY_LOADLIST LOAD_CTL
                ON  LOAD_CTL.LoadNum = ORDER_LOAD_SHIPMENT_CHARGE.entityid
                
            LEFT JOIN SHIPMENT_CHARGES_Accesorial_Importer_addon AI
                ON  AI.Entereddate30 = ORDER_LOAD_SHIPMENT_CHARGE.ActivityDateTime  -- Join Activity date time
                and AI.ENTEREDBYPARTYCODE = ORDER_LOAD_SHIPMENT_CHARGE.Employee   -- Join Employee        
                
                
        WHERE ENTITYTYPEID = 2     
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
            ,OrderSourceSystemId
            ,OrderSourceSystemName
            ,IsAutomated
            ,IsBoT
            --,ResolveNoBill
            ,CustomerId
            ,CustomerCode
            ,Mode
           -- ,ModeRollup
            ,BranchId
            ,BranchCode
            ,EmployeeBranchId
            ,EmployeeBranchCode
            ,EmployeePosition
            --,EmployeePositionRollup
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
                ,STG.ORDERSOURCESYSTEMID AS ORDERSOURCESYSTEMID
                ,STG.ORDERSOURCESYSTEMNAME AS ORDERSOURCESYSTEMNAME
                  ,CASE
                    WHEN STG.ISAUTOMATED IS NOT NULL THEN STG.ISAUTOMATED
                    WHEN (BOT.AUTOMATED = 'AUTO'
                    OR E.EMPCODE IS NULL
                    OR E.BRANCHCODE = '7650') THEN TRUE
                    ELSE FALSE
                 END AS ISAUTOMATED
                ,IFF(BOT.AUTOMATED = 'AUTO' ,TRUE ,FALSE) AS ISBOT
               --  ,STG.RESOLVENOBILL AS RESOLVENOBILL
                ,CUSTOMER.PARTYID AS CUSTOMERID
                ,UPPER(TRIM(LOADMATCH.BILLTOCOMPCODE)) AS CUSTOMERCODE
                ,UPPER(TRIM(EXECUTIONMODE.MODENAMESIMPLE)) AS MODE
                ,CUSTOMERBRANCH.PARTYID AS BRANCHID
                ,LOADMATCH.CUSTOMERBRANCH AS BRANCHCODE
                ,EMPBRANCH.PARTYID AS EMPLOYEEBRANCHID
                ,EL.BRANCHCODE AS EMPLOYEEBRANCHCODE
                ,UPPER(TRIM(EL.PSROLE)) AS EMPLOYEEPOSITION
                ,STG.SourceAsOfTS AS SourceAsOfTS
            FROM
                Base_ShipmentChargesActivity   STG              -- OPERATIONS_DOMAIN.DBO.TEMP_LOAD_ORDER_ACTIVITY STG
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
            UNION ALL
        
               SELECT
                DISTINCT                        ---- (current) Selecting distinct meaning it groups shipment with same time and employee into 1 
                STG.LOADNUM AS LOADNUM
                ,STG.ACTIVITY AS ACTIVITY
                ,STG.ACTIVITYDATETIME AS ACTIVITYDATETIME
                ,DATE(STG.ACTIVITYDATETIME) AS ACTIVITYDATE
                --,TOINTDATE(STG.ACTIVITYDATETIME) AS ACTIVITYDATEINT
                ,HOUR(STG.ACTIVITYDATETIME) AS ACTIVITYHOUR
                ,STG.ACTIVITYCOUNT AS ACTIVITYCOUNT
                ,STG.EMPLOYEE AS  EMPLOYEE
                ,STG.REASONCODE AS REASONCODE
                ,STG.ORDERSOURCESYSTEMID AS ORDERSOURCESYSTEMID
                ,STG.ORDERSOURCESYSTEMNAME AS ORDERSOURCESYSTEMNAME
                ,CASE
                    WHEN STG.ISAUTOMATED IS NOT NULL THEN STG.ISAUTOMATED
                    WHEN (BOT.AUTOMATED = 'AUTO'
                        OR E.EMPCODE IS NULL
                        OR E.BRANCHCODE = '7650') THEN TRUE
                    ELSE FALSE
                END AS ISAUTOMATED
                ,IFF(BOT.AUTOMATED = 'AUTO' ,TRUE ,FALSE) AS ISBOT       
                ,O.CUSTOMERID  AS CUSTOMERID
                ,O.CUSTOMERCODE  AS CUSTOMERCODE
                ,O.SERVICEOFFERINGDESC AS MODE
                ,O.CUSTOMERBRANCHID AS BRANCHID
                ,O.CUSTOMERBRANCHCODE AS BRANCHCODE
                ,EMPBRANCH.PARTYID AS EMPLOYEEBRANCHID
                ,EL.BRANCHCODE AS EMPLOYEEBRANCHCODE
                ,UPPER(TRIM(EL.PSROLE)) AS EMPLOYEEPOSITION
              --  ,STG.RESOLVENOBILL AS RESOLVENOBILL
                ,STG.SourceAsOfTS AS SourceAsOfTS
            FROM
                Base_ShipmentChargesActivity   STG      ---using test temp table                        ----OPERATIONS_DOMAIN.DBO.TEMP_LOAD_ORDER_ACTIVITY STG
                LEFT OUTER JOIN OPERATIONS_DOMAIN.DBO.ORDER_CHARACTERISTICS O ON
                    STG.ORDERNUM = O.ORDERNUM
                LEFT OUTER JOIN EXPRESS_RAP.DBO.EMPLOYEES E  ON
                    TRIM(STG.EMPLOYEE) = TRIM(E.EMPCODE)
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
                STG.LoadBasedActivity = FALSE

       )    
          
 SELECT
         a.employee
         ,er.subrollupfamily
         ,ba.branchname
         ,a.mode
        -- ,a.moderollup
         ,a.activity 
         ,tc.taskorder 
         ,trim(upper(ba.branchsubregion1)) as BranchRegion
         ,count(distinct a.loadnum) as loads
         ,sum(a.activitycount) as ct
       --  ,sum(a.activitycycletimeduration)/60 as hrs
         
         FROM Shipment_activity as a

LEFT JOIN TRUCKLOAD_DOMAIN.DBO.V_DIM_BRANCH as ba on ba.branchID = a.employeebranchID 
LEFT JOIN TRUCKLOAD_DOMAIN.DBO.V_DIM_CUSTOMER as c on c.customerid = a.customerid
LEFT JOIN OPERATIONS_DOMAIN.DBO.DIM_TASK_CLASSIFICATION as tc on tc.task = a.activity
LEFT JOIN  OPERATIONS_DOMAIN.DBO.DIM_EMPLOYEE_ROLE as er on trim(upper(a.employeeposition)) = trim(upper(er.psrole))
WHERE 

 ba.primarybusinesslineid = 62
 and a. IsAutomated = 'FALSE' 
 
GROUP BY        
         a.employee
         ,er.subrollupfamily
         ,ba.branchname
         ,trim(upper(ba.branchsubregion1))
       --  ,a.moderollup
         ,a.mode
         ,a.activity 
         ,tc.taskorder 
       










