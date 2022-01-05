CREATE OR REPLACE PACKAGE BODY RISGAP.RIS_INV_SOR_HARNESS_SQL
AS
--------------------------------------------------------------------------------------------------
-- Package Name  :  RIS_INV_SOR_HARNESS_SQL
-- Purpose       : The purpose of this package is generate harness data for various functionalities. 
-- Author      Date              Ver      Desc                                 
-- -------    ---------          -----    ------------------------------------ 
-- GapTech     22-Dec-2021          1.0      Initial version                      
--------------------------------------------------------------------------------------------------
      --
      L_plsql_program_name    VARCHAR2(200) := 'RIS_INV_SOR_HARNESS_SQL';
      LP_entity_name          VARCHAR2(50);
      LP_from_date            DATE;
      LP_to_date              DATE;
      LP_run_id               NUMBER(10);
      LP_location_tbl          NUMBER_TBL;
      LP_loc                  NUMBER(10);
      
      
      --
      STATUS_NEW              CONSTANT            VARCHAR2(2)   := 'N';
      STATUS_ERROR            CONSTANT            VARCHAR2(2)   := 'E';
      STATUS_COMPLETE         CONSTANT            VARCHAR2(2)   := 'C';
      
      --
      -- tran codes 
      --
      TSF_IN_TC               CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    30;
      TSF_OUT_TC              CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    32;
      BOOK_TSF_IN_TC          CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    31;
      BOOK_TSF_OUT_TC         CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    33;
      SALES_TC                CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    1;
      RETURNS_TC              CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    4;
      PO_RECEIPTS_TC          CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    20;
      SHIPMENT_RECEIPT_TC     CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    44;
      INVADJ_22_TC            CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    22;
      INVADJ_23_TC            CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    23;
      INVADJ_25_TC            CONSTANT      DAS_WV_TRAN_DATA_CODES.CODE%TYPE   :=    25;
      --
      -- BRANDS
      --
      GAP                     CONSTANT      DAS_WV_DIVISION.DIVISION%TYPE      :=    1;
      BR                      CONSTANT      DAS_WV_DIVISION.DIVISION%TYPE      :=    2;
      OLDNAVY                 CONSTANT      DAS_WV_DIVISION.DIVISION%TYPE      :=    3;
      GAPOUT                  CONSTANT      DAS_WV_DIVISION.DIVISION%TYPE      :=    4;
      BRFS                    CONSTANT      DAS_WV_DIVISION.DIVISION%TYPE      :=    5;
      NRC                     CONSTANT      DAS_WV_DIVISION.DIVISION%TYPE      :=    7;
      ATHLETA                 CONSTANT      DAS_WV_DIVISION.DIVISION%TYPE      :=    11;
      --
      ENTITY_SALES            CONSTANT      VARCHAR2(30)                       :=    'SALES';
      ENTITY_ALLOC            CONSTANT      VARCHAR2(30)                       :=    'ALLOCATION';
      ENTITY_TSF              CONSTANT      VARCHAR2(30)                       :=    'TRANSFER';
      ENTITY_PORECEIPTS       CONSTANT      VARCHAR2(30)                       :=    'PO_RECEIPTS';
      ENTITY_SHIP_RCPT        CONSTANT      VARCHAR2(30)                       :=    'SHIPMENT_RECEIPTS';
      ENTITY_INVADJ           CONSTANT      VARCHAR2(30)                       :=    'INVADJ';
      ENTITY_RETURNS          CONSTANT      VARCHAR2(30)                       :=    'RETURNS';
      ENTITY_BOOK_TSF         CONSTANT      VARCHAR2(30)                       :=    'BOOK_TRANSFER';
      PRE_JOB                 CONSTANT      VARCHAR2(30)                       :=    'PRE_JOB';  
      --
      CURSOR cur_get_sleep_config
      IS
        SELECT attribute1       process_chunk_size,
               NVL(attribute2 ,0)      sleep_time_in_secs
          FROM intf_cnfg_parm_t intf
         WHERE intf.pgm_nm        = L_plsql_program_name
           AND intf.cd_nm         = LP_entity_name
           AND intf.cnfg_typ_cd   = 'SLEEP_CONFIG';
      --
      L_tab_kafka_outbound_stg      RIS_ISOR_KAFKA_OBSTG_TAB := RIS_ISOR_KAFKA_OBSTG_TAB();
      L_tabclr_kafka_outbound_stg   RIS_ISOR_KAFKA_OBSTG_TAB := RIS_ISOR_KAFKA_OBSTG_TAB(); 
      --
      
      
--------------------------------------------------------------------------------------------------
 --  Function Name: GENERATE_CHUNKS
 --  Purpose      : Generates CHUNKs for multi-threading
--------------------------------------------------------------------------------------------------
   FUNCTION GENERATE_CHUNKS(O_error_message IN OUT     VARCHAR2,  
                            I_entity_name   IN         VARCHAR2)
   RETURN BOOLEAN
   IS
      --
      L_program_name          VARCHAR2(1000)        := 'RIS_INV_SOR_HARNESS_SQL.GENERATE_CHUNKS';
      L_mark                  VARCHAR2(200);
      L_sql                   VARCHAR2(4000);   
      L_partition_name        VARCHAR2(50);     
      --
      L_run_id                NUMBER(20);
      L_max_chunk             NUMBER(20);
      L_chunk_size            RIS_PLSQL_BATCH_CONFIG.MAX_CHUNK_SIZE%TYPE;
      
      --
      CURSOR C_next_run_id
      IS
        SELECT GAP_COMMON_RUN_ID_SEQ.NEXTVAL
          FROM dual;


   BEGIN
   
      --
      L_mark := 'Open C_next_run_id';
      --
      OPEN C_next_run_id;
      FETCH C_next_run_id INTO LP_run_id;
      CLOSE C_next_run_id;
      --
      IF LP_run_id IS NULL
      THEN
         --
         O_error_message := L_program_name || ' RUN_ID_NULL_ERROR';
         RETURN FALSE;
         --
      END IF;
      --
      L_mark := 'Cleanup ris_intf_thread';
      --
      DELETE
        FROM risgap.ris_intf_thread
       WHERE program_name = L_plsql_program_name;
      --
      L_mark := 'Insert ris_intf_thread.';
      --             
      INSERT INTO ris_intf_thread(program_name,
                                  chunk_id,
                                  key_1,
                                  key_4,
                                  status,
                                  run_id,
                                  create_datetime,
                                  create_id,
                                  last_update_datetime,
                                  last_update_id)
      SELECT DISTINCT
             L_plsql_program_name,          --program_name
             chunk_id,                      --chunk_id
             key_1,                         --loc
             I_entity_name,                 --entity_name
             STATUS_NEW,                    --status
             LP_run_id,                     --run_id
             SYSDATE,                       --create_datetime
             USER,                          --create_id
             SYSDATE,                       --last_update_datetime
             user                           --last_update_id
        FROM ris_generic_chunk_t
       WHERE entity_name = 'LOCATION';
      --
      COMMIT;
          
      --
      O_error_message := 'Program '||L_program_name||' Successfully completed for runId: '||LP_run_id;
      --
      RETURN TRUE;
      --
   EXCEPTION
   WHEN OTHERS THEN
      --
      O_error_message := SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                                              'ERROR: Unexpected Error'
                                              || SQLERRM
                                              || '; LAST PROCESSED: '
                                              || L_mark,
                                              L_program_name,
                                              TO_CHAR (SQLCODE));
      RETURN FALSE;
      --
   END GENERATE_CHUNKS;     
--------------------------------------------------------------------------------------------------
-- Function Name : INSERT_KAFKA_STAGE
-- Purpose       : Loads the data into respective stage tables from gttS.
--------------------------------------------------------------------------------------------------
   FUNCTION INSERT_KAFKA_STAGE(O_error_message   IN OUT   VARCHAR2)
   RETURN BOOLEAN
   IS
      --
      L_program_name                VARCHAR2(1000)    := 'RIS_INV_SOR_HARNESS_SQL.INSERT_KAFKA_STAGE';
      L_mark                        VARCHAR2(1000);
      --                                
   BEGIN
      --
      L_mark := 'Inserting the data into ISOR kafka outbound stage tables.';
      --
      INSERT ALL 
      WHEN brand = GAP THEN
      INTO ris_isor_gap_kafka_outbound_stage(ID          ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
                                             values(ID   ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
      WHEN brand = BR THEN
      INTO ris_isor_br_kafka_outbound_stage (ID          ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
                                             values(ID   ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
      WHEN brand = OLDNAVY THEN
      INTO ris_isor_on_kafka_outbound_stage(ID          ,
                                            RAW_PAYLOAD , 
                                            STATUS      , 
                                            CRT_USER    , 
                                            CRT_DTTM    , 
                                            UPD_USER    , 
                                            UPD_DTTM)      
                                            values(ID   ,
                                            RAW_PAYLOAD , 
                                            STATUS      , 
                                            CRT_USER    , 
                                            CRT_DTTM    , 
                                            UPD_USER    , 
                                            UPD_DTTM)                                           
      WHEN brand = GAPOUT THEN
      INTO ris_isor_gapout_kafka_outbound_stage(ID       ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM) 
                                             values(ID   ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)                                           
      WHEN brand = BRFS THEN
      INTO ris_isor_brfs_kafka_outbound_stage(ID         ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
                                             values(ID   ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
      WHEN brand = NRC THEN
      INTO ris_isor_nrc_kafka_outbound_stage(ID          ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
                                             values(ID   ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
      WHEN brand = ATHLETA THEN
      INTO ris_isor_athleta_kafka_outbound_stage(ID      ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
                                             values(ID   ,
                                             RAW_PAYLOAD , 
                                             STATUS      , 
                                             CRT_USER    , 
                                             CRT_DTTM    , 
                                             UPD_USER    , 
                                             UPD_DTTM)
      SELECT  event_id id,
              brand,
              json_array(json_object(
               key 'ID'                                  value event_id,
               key 'INV_TXN_TYPE'                        value event_inv_txn_type,
               key 'INV_SUBTXN_TYPE'                     value event_inv_sub_txn_type,
               key 'PUBLISH_TIME'                        value event_db_outbound_time,
               key 'EVENTATTRIBUTES'                     value json_object(key 'ATTR1' value NVL(event_attr1,-1),
                                                                           key 'ATTR2' value NVL(event_attr2,-1)),
               key 'LOCATION'                            value location,
               key 'LOCATION_TYPE'                       value location_type,
               key 'PHYSICAL_WH'                         value physical_wh,
               key 'CHANNEL'                             value channel,
               key 'ITEM'                                value legacy_item,
               key 'INTELLIGENT_CC'                      value legacy_cc,
               key 'REF_ITEM'                            value global_item,
               key 'PACKIND'                             value packind,
               key 'ITEM_PARENT'                         value item_parent,
               key 'BMC'                                 value json_object(key 'Brand'   value brand,
                                                                           key 'Market'  value market,
                                                                           key 'Channel' value channel),
               key 'STOCK_ON_HAND'                       value stock_on_hand,
               key 'INTRANSIT_QUANTITY'                  value in_transit_quantity,
               key 'PACKCOMP_INTRANSIT_QUANTITY'         value pack_comp_intransit_quantity,
               key 'PACKCOMP_STOCK_ON_HAND'              value pack_comp_stock_on_hand,
               key 'TSFALLOC_RESERVED_QUANTITY'          value tsf_alloc_reserved_quantity,
               key 'PACKCOMP_TSFALLOC_RESERVED_QUANTITY' value pack_comp_tsf_alloc_reserved_quantity,
               key 'TSFALLOC_EXPECTED_QUANTITY'          value tsf_alloc_expected_quantity,
               key 'PACKCOMP_TSFALLOC_EXPECTED_QUANTITY' value pack_comp_tsf_alloc_expected_quantity,
               key 'NONSELLABLE_QUANTITY'                value non_sellable_quantity,
               key 'CUSTOMER_RESERVED_QUANTITY'          value customer_reserved_quantity,
               key 'CUSTOMER_BACKORDER_QUANTITY'         value customer_backorder_quantity,
               key 'ONORDER_QUANTITY'                    value on_order_qty,
               key 'INPROGRESS_QUANTITY'                 value null) RETURNING CLOB) raw_payload,
               STATUS_NEW status,
               user CRT_USER,     
               sysdate CRT_DTTM,
               user UPD_USER,
               sysdate UPD_DTTM
        FROM TABLE(L_tab_kafka_outbound_stg);
      --
      COMMIT;
      --
      RETURN TRUE;
      --
   EXCEPTION
   WHEN OTHERS
   THEN
      --
      O_error_message := SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                                             'ERROR: Unexpected Error'
                                             ||SQLERRM
                                             ||'; LAST PROCESSED: '
                                             ||L_mark,
                                             L_program_name,
                                             TO_CHAR (SQLCODE));

      RETURN FALSE;
      --
   END INSERT_KAFKA_STAGE;
--------------------------------------------------------------------------------------------------
-- Function Name : LOAD_GTT
-- Purpose       : Loads the data into respective stage tables from ods_tran_data_history.
--------------------------------------------------------------------------------------------------
   FUNCTION LOAD_GTT(O_error_message   IN OUT   VARCHAR2)
   RETURN BOOLEAN
   IS
      --
      L_program_name                VARCHAR2(1000)    := 'RIS_INV_SOR_HARNESS_SQL.LOAD_GTT';
      L_mark                        VARCHAR2(1000);
      L_sql                         VARCHAR2(4000)    := NULL;
      
      CURSOR cur_get_prejob_configs
      IS
       SELECT from_date,
              to_date
         FROM ris_system_variables intf
        WHERE intf.program_name = L_plsql_program_name
          AND intf.system_code  = PRE_JOB;
      --
      L_rec_prejob_configs   cur_get_prejob_configs%ROWTYPE;     
      --
   BEGIN
      --
      --
      L_mark := 'Open cur_get_prejob_configs.';
      --
       OPEN cur_get_prejob_configs;
      FETCH cur_get_prejob_configs INTO L_rec_prejob_configs;
      IF cur_get_prejob_configs%NOTFOUND
      THEN
         --
         O_error_message := 'Error: Not able to fetch the intf_cnfg_parm_t configs.'||L_mark||L_program_name;
         RETURN FALSE;
         --
      END IF;
      CLOSE cur_get_prejob_configs;
      
      IF LP_entity_name = ENTITY_SALES
      THEN
         --
         L_mark := 'Insert into ris_isor_trandata_gtt for entity: '||ENTITY_SALES;
         -- 
         INSERT INTO ris_isor_trandata_gtt( item
                                           ,dept
                                           ,pack_ind
                                           ,location
                                           ,loc_type
                                           ,units
                                           ,ref_no_1
                                           ,ref_no_2
                                           ,ref_pack_no
                                           ,gl_ref_no)
         SELECT item
               ,dept
               ,NVL(pack_ind,'N') pack_ind
               ,location
               ,loc_type
               ,units
               ,ref_no_1
               ,ref_no_2
               ,ref_pack_no
               ,gl_ref_no
           FROM ods_tran_data_history  tdh,
                TABLE(CAST(LP_location_tbl AS NUMBER_TBL)) t
          WHERE tdh.tran_code = SALES_TC
            AND tdh.location = value(t)
            AND tdh.post_date BETWEEN L_rec_prejob_configs.from_date AND L_rec_prejob_configs.to_date; 

         --
         L_mark := 'Insert into ris_isor_cit_trandata_gtt for entity: '||ENTITY_SALES;
         --
         INSERT INTO ris_isor_cit_trandata_gtt(event_id, 
                                               event_inv_txn_type, 
                                               event_db_outbound_time, 
                                               event_attr1,
                                               event_attr2,
                                               location, 
                                               location_type, 
                                               physical_wh, 
                                               channel, 
                                               global_item, 
                                               packind, 
                                               item_parent, 
                                               color, 
                                               size_cd, 
                                               brand, 
                                               market, 
                                               legacy_item, 
                                               legacy_pack, 
                                               legacy_cc, 
                                               stock_on_hand)
         SELECT RIS_INV_SOR_ENVENTID_SEQ.NEXTVAL       event_id,
                ENTITY_SALES                           event_inv_txn_type,
                SYSDATE                                event_db_outbound_time,
                gtt.ref_no_1                           event_attr1,
                gtt.ref_no_2                           event_attr2,                                                     
                gtt.location                           location, 
                gtt.loc_type                           location_type, 
                DECODE(gtt.loc_type,'W',
                            cit.legacy_loc,NULL)            physical_wh, 
                CASE 
                WHEN cit.chnl_id = 1
                THEN 'RETAIL'
                WHEN cit.chnl_id = 2
                THEN 'ONLINE'
                WHEN cit.chnl_id = 3
                THEN 'OUTLET'
                ELSE NULL
                END                                    channel,
                gtt.item                               global_item,
                gtt.pack_ind                           packind,
                cit.glbl_itm_sty_nbr                   item_parent,
                cit.sty_clr_cd                         color,
                cit.lgcy_size_cd                       size_cd,
                cit.brd_id                             brand,
                cit.mkt_id                             market,
                REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                cit.legacy_pack_no                     legacy_pack,
                cit.lgcy_product_no                    legacy_cc,
                (-1)* units                            stock_on_hand           
           FROM gap_ils_cit_extract_t cit,
                ris_isor_trandata_gtt gtt
          WHERE cit.status    = 'V'
            AND cit.item      = gtt.item
            AND cit.location  = gtt.location;
         --
     ELSIF LP_entity_name = ENTITY_ALLOC
      THEN
         --
         L_mark := 'Insert into ris_isor_trandata_gtt for entity: '||ENTITY_ALLOC;
         -- 
         INSERT INTO ris_isor_trandata_gtt( item
                                           ,dept
                                           ,pack_ind
                                           ,location
                                           ,loc_type
                                           ,units
                                           ,ref_no_1
                                           ,ref_no_2
                                           ,ref_pack_no
                                           ,gl_ref_no)
         SELECT /*+PARALLEL(tdh,8)*/ item
               ,dept
               ,NVL(pack_ind,'N') pack_ind
               ,location
               ,loc_type
               ,units
               ,ref_no_1
               ,ref_no_2
               ,ref_pack_no
               ,gl_ref_no
           FROM ods_tran_data_history tdh,
                TABLE(CAST(LP_location_tbl AS NUMBER_TBL)) t           
          WHERE tdh.tran_code IN(TSF_IN_TC,TSF_OUT_TC)
            AND tdh.location = value(t)
            AND tdh.post_date BETWEEN L_rec_prejob_configs.from_date AND L_rec_prejob_configs.to_date; 
         --
         L_mark := 'Insert into ris_isor_cit_trandata_gtt for entity: '||ENTITY_ALLOC;
         --
         INSERT INTO ris_isor_cit_trandata_gtt(event_id, 
                                               event_inv_txn_type,
                                               event_inv_sub_txn_type,  
                                               event_db_outbound_time, 
                                               event_attr1,
                                               event_attr2,
                                               location, 
                                               location_type, 
                                               physical_wh, 
                                               channel, 
                                               global_item, 
                                               packind, 
                                               item_parent, 
                                               color, 
                                               size_cd, 
                                               brand, 
                                               market, 
                                               legacy_item, 
                                               legacy_pack, 
                                               legacy_cc, 
                                               stock_on_hand,
                                               pack_comp_stock_on_hand,
                                               in_transit_quantity,
                                               pack_comp_intransit_quantity,
                                               tsf_alloc_reserved_quantity,
                                               pack_comp_tsf_alloc_reserved_quantity,
                                               tsf_alloc_expected_quantity,
                                               pack_comp_tsf_alloc_expected_quantity)
         WITH ALLOC_DATA AS(
               SELECT ENTITY_ALLOC                           event_inv_txn_type,
                      'SHIPMENT'                             event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                         
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)       physical_wh, 
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      cit.legacy_pack_no                     legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    stock_on_hand,
                      NULL                                   pack_comp_stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units
                      END                                    in_transit_quantity,
                      NULL                                   pack_comp_intransit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    tsf_alloc_reserved_quantity,
                      NULL                                   pack_comp_tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units
                      END                                   tsf_alloc_expected_quantitY,
                      NULL                                  pack_comp_tsf_alloc_expected_quantity
                 FROM gap_ils_cit_extract_t cit,
                      ris_isor_trandata_gtt gtt
                WHERE gtt.pack_ind  = 'N'
                  AND cit.status    = 'V'
                  AND cit.item      = gtt.item
                  AND cit.location  = gtt.location
               UNION ALL
               SELECT ENTITY_ALLOC                           event_inv_txn_type,
                      'SHIPMENT'                             event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,   
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)            physical_wh, 
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      to_char(xref.ppk_nbr)                  legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units*pk.pack_qty
                      END                                    pack_comp_stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units
                      END                                    in_transit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units*pk.pack_qty
                      END                                    pack_comp_intransit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units*pk.pack_qty
                      END                                    pack_comp_tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units
                      END                                    tsf_alloc_expected_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units*pk.pack_qty
                      END                                    pack_comp_tsf_alloc_expected_quantity 
                 FROM gap_ils_cit_extract_t cit,
                      ris_isor_trandata_gtt gtt,
                      packitem pk,
                      tppx_ppk_xref xref
                WHERE gtt.pack_ind    = 'Y'
                  AND gtt.ref_pack_no = pk.pack_no(+)
                  AND gtt.ref_pack_no = xref.pack_no(+)
                  AND cit.status      = 'V'
                  AND cit.item        = gtt.item
                  AND cit.location    = gtt.location)
         SELECT RIS_INV_SOR_ENVENTID_SEQ.NEXTVAL       event_id,
                event_inv_txn_type,
                event_inv_sub_txn_type,
                event_db_outbound_time,
                event_attr1,
                event_attr2,
                location, 
                location_type, 
                physical_wh, 
                channel,
                global_item,
                packind,
                item_parent,
                color,
                size_cd,
                brand,
                market,
                legacy_item,
                legacy_pack,
                legacy_cc,
                stock_on_hand,
                pack_comp_stock_on_hand,
                in_transit_quantity,
                pack_comp_intransit_quantity,
                tsf_alloc_reserved_quantity,
                pack_comp_tsf_alloc_reserved_quantity,
                tsf_alloc_expected_quantity,
                pack_comp_tsf_alloc_expected_quantity                
           FROM ALLOC_DATA;
         --
      ELSIF LP_entity_name = ENTITY_TSF
      THEN
         --
         L_mark := 'Insert into ris_isor_trandata_gtt for entity: '||ENTITY_TSF;
         -- 
         INSERT INTO ris_isor_trandata_gtt( item
                                           ,dept
                                           ,pack_ind
                                           ,location
                                           ,loc_type
                                           ,units
                                           ,ref_no_1
                                           ,ref_no_2
                                           ,ref_pack_no
                                           ,gl_ref_no)
         SELECT item
               ,dept
               ,NVL(pack_ind,'N') pack_ind
               ,location
               ,loc_type
               ,units
               ,ref_no_1
               ,ref_no_2
               ,ref_pack_no
               ,gl_ref_no
           FROM ods_tran_data_history tdh,
                TABLE(CAST(LP_location_tbl AS NUMBER_TBL)) t           
          WHERE tdh.tran_code IN(TSF_IN_TC,TSF_OUT_TC)
            AND tdh.location = value(t)
            AND tdh.post_date BETWEEN L_rec_prejob_configs.from_date AND L_rec_prejob_configs.to_date; 
         --
         L_mark := 'Insert into ris_isor_cit_trandata_gtt for entity: '||ENTITY_TSF;
         --
         INSERT INTO ris_isor_cit_trandata_gtt(event_id, 
                                               event_inv_txn_type,
                                               event_inv_sub_txn_type,  
                                               event_db_outbound_time, 
                                               event_attr1,
                                               event_attr2,
                                               location, 
                                               location_type, 
                                               physical_wh, 
                                               channel, 
                                               global_item, 
                                               packind, 
                                               item_parent, 
                                               color, 
                                               size_cd, 
                                               brand, 
                                               market, 
                                               legacy_item, 
                                               legacy_pack, 
                                               legacy_cc, 
                                               stock_on_hand,
                                               pack_comp_stock_on_hand,
                                               in_transit_quantity,
                                               pack_comp_intransit_quantity,
                                               tsf_alloc_reserved_quantity,
                                               pack_comp_tsf_alloc_reserved_quantity,
                                               tsf_alloc_expected_quantity,
                                               pack_comp_tsf_alloc_expected_quantity)
         WITH TSF_DATA AS(
               SELECT ENTITY_TSF                       event_inv_txn_type,
                      'SHIPMENT'                             event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                                             
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                    cit.legacy_loc,NULL)            physical_wh, 
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      cit.legacy_pack_no                     legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    stock_on_hand,
                      NULL                                   pack_comp_stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units
                      END                                    in_transit_quantity,
                      NULL                                   pack_comp_intransit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    tsf_alloc_reserved_quantity,
                      NULL                                   pack_comp_tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units
                      END                                   tsf_alloc_expected_quantitY,
                      NULL                                  pack_comp_tsf_alloc_expected_quantity
                 FROM gap_ils_cit_extract_t cit,
                      ris_isor_trandata_gtt gtt
                WHERE gtt.pack_ind  = 'N'
                  AND cit.status    = 'V'
                  AND cit.item      = gtt.item
                  AND cit.location  = gtt.location
               UNION ALL
               SELECT ENTITY_TSF                             event_inv_txn_type,
                      'SHIPMENT'                             event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                                             
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)            physical_wh,
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      to_char(xref.ppk_nbr)                  legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units*pk.pack_qty
                      END                                    pack_comp_stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units
                      END                                    in_transit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units*pk.pack_qty
                      END                                    pack_comp_intransit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units*pk.pack_qty
                      END                                    pack_comp_tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units
                      END                                    tsf_alloc_expected_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units*pk.pack_qty
                      END                                    pack_comp_tsf_alloc_expected_quantity 
                 FROM gap_ils_cit_extract_t cit,
                      ris_isor_trandata_gtt gtt,
                      packitem pk,
                      tppx_ppk_xref xref
                WHERE gtt.pack_ind    = 'Y'
                  AND gtt.ref_pack_no = pk.pack_no(+)
                  AND gtt.ref_pack_no = xref.pack_no(+)
                  AND cit.status      = 'V'
                  AND cit.item        = gtt.item
                  AND cit.location    = gtt.location)
         SELECT RIS_INV_SOR_ENVENTID_SEQ.NEXTVAL       event_id,
                event_inv_txn_type,
                event_inv_sub_txn_type,
                event_db_outbound_time,
                event_attr1,
                event_attr2,
                location, 
                location_type, 
                physical_wh, 
                channel,
                global_item,
                packind,
                item_parent,
                color,
                size_cd,
                brand,
                market,
                legacy_item,
                legacy_pack,
                legacy_cc,
                stock_on_hand,
                pack_comp_stock_on_hand,
                in_transit_quantity,
                pack_comp_intransit_quantity,
                tsf_alloc_reserved_quantity,
                pack_comp_tsf_alloc_reserved_quantity,
                tsf_alloc_expected_quantity,
                                               pack_comp_tsf_alloc_expected_quantity                
           FROM TSF_DATA;
         --
      ELSIF LP_entity_name = ENTITY_PORECEIPTS
      THEN
         --
         L_mark := 'Insert into ris_isor_trandata_gtt for entity: '||ENTITY_PORECEIPTS;
         -- 
         INSERT INTO ris_isor_trandata_gtt( item
                                           ,dept
                                           ,pack_ind
                                           ,location
                                           ,loc_type
                                           ,units
                                           ,ref_no_1
                                           ,ref_no_2
                                           ,ref_pack_no
                                           ,gl_ref_no)
         SELECT  item
                ,dept
                ,NVL(pack_ind,'N') pack_ind
                ,location
                ,loc_type
                ,units
                ,ref_no_1
                ,ref_no_2
                ,ref_pack_no
                ,gl_ref_no
           FROM ods_tran_data_history tdh,
                TABLE(CAST(LP_location_tbl AS NUMBER_TBL)) t                        
          WHERE tdh.tran_code = PO_RECEIPTS_TC
            AND tdh.location = value(t)
            AND tdh.post_date BETWEEN L_rec_prejob_configs.from_date AND L_rec_prejob_configs.to_date; 
         --
         L_mark := 'Insert into ris_isor_cit_trandata_gtt for entity: '||ENTITY_PORECEIPTS;
         --
         INSERT INTO ris_isor_cit_trandata_gtt(event_id, 
                                               event_inv_txn_type,
                                               event_inv_sub_txn_type,
                                               event_db_outbound_time, 
                                               event_attr1,
                                               event_attr2,
                                               location, 
                                               location_type, 
                                               physical_wh, 
                                               channel, 
                                               global_item, 
                                               packind, 
                                               item_parent, 
                                               color, 
                                               size_cd, 
                                               brand, 
                                               market, 
                                               legacy_item, 
                                               legacy_pack, 
                                               legacy_cc, 
                                               stock_on_hand)
         WITH PO_DATA AS(                                              
               SELECT 'ORDER'                                event_inv_txn_type,
                      'RECEIPT'                              event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                                             
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)         physical_wh,  
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      cit.legacy_pack_no                     legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      gtt.units                              stock_on_hand,
                      NULL                                   pack_comp_stock_on_hand,
                      (-1)*gtt.units                         on_order_qty               
                 FROM gap_ils_cit_extract_t cit,
                      ris_isor_trandata_gtt gtt
                WHERE gtt.pack_ind  = 'N'
                  AND cit.status    = 'V'
                  AND cit.item      = gtt.item
                  AND cit.location  = gtt.location
               UNION ALL
               SELECT 'ORDER'                                event_inv_txn_type,
                      'RECEIPT'                              event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                                             
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)            physical_wh,  
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      to_char(xref.ppk_nbr)                  legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      gtt.units                              stock_on_hand,
                      gtt.units*pk.pack_qty                  pack_comp_stock_on_hand,
                      (-1)*gtt.units                         on_order_qty               
                 FROM gap_ils_cit_extract_t cit,
                      packitem pk,
                      tppx_ppk_xref xref,
                      ris_isor_trandata_gtt gtt
                WHERE gtt.pack_ind    = 'Y'
                  AND gtt.ref_pack_no = pk.pack_no(+)
                  AND gtt.ref_pack_no = xref.pack_no(+)
                  AND cit.status      = 'V'
                  AND cit.item        = gtt.item
                  AND cit.location    = gtt.location)
         SELECT RIS_INV_SOR_ENVENTID_SEQ.NEXTVAL       event_id,
                event_inv_txn_type,
                event_inv_sub_txn_type,
                event_db_outbound_time,
                event_attr1,
                event_attr2,
                location, 
                location_type, 
                physical_wh, 
                channel,
                global_item,
                packind,
                item_parent,
                color,
                size_cd,
                brand,
                market,
                legacy_item,
                legacy_pack,
                legacy_cc,
                stock_on_hand   
           FROM PO_DATA;
         --
      ELSIF LP_entity_name = ENTITY_SHIP_RCPT
      THEN
         --
         L_mark := 'Insert into ris_isor_trandata_gtt for entity: '||ENTITY_SHIP_RCPT;
         -- 
         INSERT INTO ris_isor_trandata_gtt( item
                                           ,dept
                                           ,pack_ind
                                           ,location
                                           ,loc_type
                                           ,units
                                           ,ref_no_1
                                           ,ref_no_2
                                           ,ref_pack_no
                                           ,gl_ref_no)
         SELECT  item
                ,dept
                ,NVL(pack_ind,'N') pack_ind
                ,location
                ,loc_type
                ,units
                ,ref_no_1
                ,ref_no_2
                ,ref_pack_no
                ,gl_ref_no
           FROM ods_tran_data_history tdh,
                TABLE(CAST(LP_location_tbl AS NUMBER_TBL)) t                   
          WHERE tdh.tran_code = SHIPMENT_RECEIPT_TC
            AND tdh.location = value(t)
            AND tdh.post_date BETWEEN L_rec_prejob_configs.from_date AND L_rec_prejob_configs.to_date; 
         --
         L_mark := 'Insert into ris_isor_cit_trandata_gtt for entity: '||ENTITY_SHIP_RCPT;
         --
         INSERT INTO ris_isor_cit_trandata_gtt(event_id, 
                                               event_inv_txn_type, 
                                               event_inv_sub_txn_type,
                                               event_db_outbound_time, 
                                               event_attr1,
                                               event_attr2,
                                               location, 
                                               location_type, 
                                               physical_wh, 
                                               channel, 
                                               global_item, 
                                               packind, 
                                               item_parent, 
                                               color, 
                                               size_cd, 
                                               brand, 
                                               market, 
                                               legacy_item, 
                                               legacy_pack, 
                                               legacy_cc, 
                                               stock_on_hand)
         WITH SHIPMENT_DATA AS(                                            
               SELECT 'SHIPMENT'                             event_inv_txn_type,
                      'RECEIPT'                              event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                                             
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)            physical_wh,
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      cit.legacy_pack_no                     legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      CASE
                      WHEN EXISTS(SELECT 1
                                    FROM shipment sh
                                   WHERE sh.shipment = gtt.ref_no_2
                                     AND sh.to_loc   = gtt.location
                                     AND sh.status_code   = 'I')
                      THEN gtt.units
                      END                                    stock_on_hand,
                      NULL                                   pack_comp_stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1
                                    FROM shipment sh
                                   WHERE sh.shipment = gtt.ref_no_2
                                     AND sh.to_loc   = gtt.location
                                     AND sh.status_code   = 'I')
                      THEN (-1)*gtt.units
                      END                                    in_transit_quantity,
                      NULL                                   pack_comp_intransit_quantity
                 FROM gap_ils_cit_extract_t cit,
                      ris_isor_trandata_gtt gtt
                WHERE gtt.pack_ind  = 'N'
                  AND cit.status    = 'V'
                  AND cit.item      = gtt.item
                  AND cit.location  = gtt.location
               UNION ALL
               SELECT 'SHIPMENT'                             event_inv_txn_type,
                      'RECEIPT'                              event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                                             
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)            physical_wh, 
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      to_char(xref.ppk_nbr)                  legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      CASE
                      WHEN EXISTS(SELECT 1
                                    FROM shipment sh
                                   WHERE sh.shipment = gtt.ref_no_2
                                     AND sh.to_loc   = gtt.location
                                     AND sh.status_code   = 'I')
                      THEN gtt.units
                      END                                    stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1
                                    FROM shipment sh
                                   WHERE sh.shipment = gtt.ref_no_2
                                     AND sh.to_loc   = gtt.location
                                     AND sh.status_code   = 'I')
                      THEN gtt.units*pk.pack_qty
                      END                                    pack_comp_stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1
                                    FROM shipment sh
                                   WHERE sh.shipment = gtt.ref_no_2
                                     AND sh.to_loc   = gtt.location
                                     AND sh.status_code   = 'I')
                      THEN (-1)*gtt.units
                      END                                    in_transit_quantity,
                                     CASE
                      WHEN EXISTS(SELECT 1
                                    FROM shipment sh
                                   WHERE sh.shipment = gtt.ref_no_2
                                     AND sh.to_loc   = gtt.location
                                     AND sh.status_code   = 'I')
                      THEN (-1)*gtt.units*pk.pack_qty
                      END                                    pack_comp_intransit_quantity
                 FROM gap_ils_cit_extract_t cit,
                      packitem pk,
                      tppx_ppk_xref xref,
                      ris_isor_trandata_gtt gtt
                WHERE gtt.pack_ind    = 'Y'
                  AND gtt.ref_pack_no = pk.pack_no(+)
                  AND gtt.ref_pack_no = xref.pack_no(+)
                  AND cit.status      = 'V'
                  AND cit.item        = gtt.item
                  AND cit.location    = gtt.location)
         SELECT RIS_INV_SOR_ENVENTID_SEQ.NEXTVAL       event_id,
                event_inv_txn_type,
                event_inv_sub_txn_type,
                event_db_outbound_time,
                event_attr1,
                event_attr2,
                location, 
                location_type, 
                physical_wh, 
                channel,
                global_item,
                packind,
                item_parent,
                color,
                size_cd,
                brand,
                market,
                legacy_item,
                legacy_pack,
                legacy_cc,
                stock_on_hand   
           FROM SHIPMENT_DATA;
         --
      ELSIF LP_entity_name = ENTITY_INVADJ
      THEN
         --
         L_mark := 'Insert into ris_isor_trandata_gtt for entity: '||ENTITY_INVADJ;
         -- 
         INSERT INTO ris_isor_trandata_gtt( item
                                           ,dept
                                           ,pack_ind
                                           ,location
                                           ,loc_type
                                           ,units
                                           ,ref_no_1
                                           ,ref_no_2
                                           ,ref_pack_no
                                           ,gl_ref_no)
         SELECT  item
                ,dept
                ,NVL(pack_ind,'N') pack_ind
                ,location
                ,loc_type
                ,units
                ,ref_no_1
                ,ref_no_2
                ,ref_pack_no
                ,gl_ref_no
           FROM ods_tran_data_history tdh,
                TABLE(CAST(LP_location_tbl AS NUMBER_TBL)) t                           
          WHERE tdh.tran_code IN(INVADJ_22_TC,INVADJ_23_TC)
            AND tdh.location = value(t)
            AND tdh.post_date BETWEEN L_rec_prejob_configs.from_date AND L_rec_prejob_configs.to_date; 
         --
         --
         L_mark := 'Insert into ris_isor_cit_trandata_gtt for entity: '||ENTITY_INVADJ;
         --
         INSERT INTO ris_isor_cit_trandata_gtt(event_id, 
                                               event_inv_txn_type, 
                                               event_db_outbound_time, 
                                               event_attr1,
                                               event_attr2,
                                               location, 
                                               location_type, 
                                               physical_wh, 
                                               channel, 
                                               global_item, 
                                               packind, 
                                               item_parent, 
                                               color, 
                                               size_cd, 
                                               brand, 
                                               market, 
                                               legacy_item, 
                                               legacy_pack, 
                                               legacy_cc, 
                                               stock_on_hand)
         SELECT RIS_INV_SOR_ENVENTID_SEQ.NEXTVAL       event_id,
                ENTITY_INVADJ                          event_inv_txn_type,
                SYSDATE                                event_db_outbound_time,
                gtt.ref_no_1                           event_attr1,
                gtt.ref_no_2                           event_attr2,                                                     
                gtt.location                           location, 
                gtt.loc_type                           location_type, 
                DECODE(gtt.loc_type,'W',
                            cit.legacy_loc,NULL)            physical_wh, 
                CASE 
                WHEN cit.chnl_id = 1
                THEN 'RETAIL'
                WHEN cit.chnl_id = 2
                THEN 'ONLINE'
                WHEN cit.chnl_id = 3
                THEN 'OUTLET'
                ELSE NULL
                END                                    channel,
                gtt.item                               global_item,
                gtt.pack_ind                           packind,
                cit.glbl_itm_sty_nbr                   item_parent,
                cit.sty_clr_cd                         color,
                cit.lgcy_size_cd                       size_cd,
                cit.brd_id                             brand,
                cit.mkt_id                             market,
                REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                cit.legacy_pack_no                     legacy_pack,
                cit.lgcy_product_no                    legacy_cc,
                gtt.units                              stock_on_hand           
           FROM gap_ils_cit_extract_t cit,
                ris_isor_trandata_gtt gtt
          WHERE cit.item      = gtt.item
            AND cit.location  = gtt.location;         
         --
      ELSIF LP_entity_name = ENTITY_RETURNS
      THEN
         --
         L_mark := 'Insert into ris_isor_trandata_gtt for entity: '||ENTITY_RETURNS;
         -- 
         INSERT INTO ris_isor_trandata_gtt( item
                                           ,dept
                                           ,pack_ind
                                           ,location
                                           ,loc_type
                                           ,units
                                           ,ref_no_1
                                           ,ref_no_2
                                           ,ref_pack_no
                                           ,gl_ref_no)
         SELECT  item
                ,dept
                ,NVL(pack_ind,'N') pack_ind
                ,location
                ,loc_type
                ,units
                ,ref_no_1
                ,ref_no_2
                ,ref_pack_no
                ,gl_ref_no
           FROM ods_tran_data_history tdh,
                TABLE(CAST(LP_location_tbl AS NUMBER_TBL)) t                           
          WHERE tdh.tran_code = RETURNS_TC
            AND tdh.location =  value(t)
            AND tdh.post_date BETWEEN L_rec_prejob_configs.from_date AND L_rec_prejob_configs.to_date; 
         --
         L_mark := 'Insert into ris_isor_cit_trandata_gtt for entity: '||ENTITY_RETURNS;
         --
         INSERT INTO ris_isor_cit_trandata_gtt(event_id, 
                                               event_inv_txn_type, 
                                               event_db_outbound_time, 
                                               event_attr1,
                                               event_attr2,
                                               location, 
                                               location_type, 
                                               physical_wh, 
                                               channel, 
                                               global_item, 
                                               packind, 
                                               item_parent, 
                                               color, 
                                               size_cd, 
                                               brand, 
                                               market, 
                                               legacy_item, 
                                               legacy_pack, 
                                               legacy_cc, 
                                               stock_on_hand)
         SELECT RIS_INV_SOR_ENVENTID_SEQ.NEXTVAL       event_id,
                ENTITY_RETURNS                         event_inv_txn_type,
                SYSDATE                                event_db_outbound_time,
                gtt.ref_no_1                           event_attr1,
                gtt.ref_no_2                           event_attr2,                                       
                gtt.location                           location, 
                gtt.loc_type                           location_type, 
                DECODE(gtt.loc_type,'W',
                            cit.legacy_loc,NULL)            physical_wh,
                CASE 
                WHEN cit.chnl_id = 1
                THEN 'RETAIL'
                WHEN cit.chnl_id = 2
                THEN 'ONLINE'
                WHEN cit.chnl_id = 3
                THEN 'OUTLET'
                ELSE NULL
                END                                    channel,
                gtt.item                               global_item,
                gtt.pack_ind                           packind,
                cit.glbl_itm_sty_nbr                   item_parent,
                cit.sty_clr_cd                         color,
                cit.lgcy_size_cd                       size_cd,
                cit.brd_id                             brand,
                cit.mkt_id                             market,
                REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                cit.legacy_pack_no                     legacy_pack,
                cit.lgcy_product_no                    legacy_cc,
                gtt.units                              stock_on_hand           
           FROM gap_ils_cit_extract_t cit,
                ris_isor_trandata_gtt gtt
          WHERE cit.item      = gtt.item
            AND cit.location  = gtt.location;
         --
      ELSIF LP_entity_name = ENTITY_BOOK_TSF
      THEN
         --
         L_mark := 'Insert into ris_isor_trandata_gtt for entity: '||ENTITY_BOOK_TSF;
         -- 
         INSERT INTO ris_isor_trandata_gtt( item
                                           ,dept
                                           ,pack_ind
                                           ,location
                                           ,loc_type
                                           ,units
                                           ,ref_no_1
                                           ,ref_no_2
                                           ,ref_pack_no
                                           ,gl_ref_no)
         SELECT  item
                ,dept
                ,NVL(pack_ind,'N') pack_ind
                ,location
                ,loc_type
                ,units
                ,ref_no_1
                ,ref_no_2
                ,ref_pack_no
                ,gl_ref_no
           FROM ods_tran_data_history tdh,
                TABLE(CAST(LP_location_tbl AS NUMBER_TBL)) t                           
          WHERE tdh.tran_code IN(BOOK_TSF_IN_TC,BOOK_TSF_OUT_TC)
            AND tdh.location = value(t)
            AND tdh.post_date BETWEEN L_rec_prejob_configs.from_date AND L_rec_prejob_configs.to_date; 
         --
         --
         L_mark := 'Insert into ris_isor_cit_trandata_gtt for entity: '||ENTITY_BOOK_TSF;
         --
         INSERT INTO ris_isor_cit_trandata_gtt(event_id, 
                                               event_inv_txn_type,
                                               event_inv_sub_txn_type,  
                                               event_db_outbound_time, 
                                               event_attr1,
                                               event_attr2,
                                               location, 
                                               location_type, 
                                               physical_wh, 
                                               channel, 
                                               global_item, 
                                               packind, 
                                               item_parent, 
                                               color, 
                                               size_cd, 
                                               brand, 
                                               market, 
                                               legacy_item, 
                                               legacy_pack, 
                                               legacy_cc, 
                                               stock_on_hand,
                                               pack_comp_stock_on_hand,
                                               in_transit_quantity,
                                               pack_comp_intransit_quantity,
                                               tsf_alloc_reserved_quantity,
                                               pack_comp_tsf_alloc_reserved_quantity,
                                               tsf_alloc_expected_quantity,
                                               pack_comp_tsf_alloc_expected_quantity)
         WITH BOOKTSF_DATA AS(                                             
               SELECT ENTITY_BOOK_TSF                        event_inv_txn_type,
                      'SHIPMENT'                             event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                                             
                      gtt.location                           location, 
                      gtt.loc_type                           location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)         physical_wh, 
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      cit.legacy_pack_no                     legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    stock_on_hand,
                      NULL                                   pack_comp_stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units
                      END                                    in_transit_quantity,
                      NULL                                   pack_comp_intransit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    tsf_alloc_reserved_quantity,
                      NULL                                   pack_comp_tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units
                      END                                   tsf_alloc_expected_quantitY,
                      NULL                                  pack_comp_tsf_alloc_expected_quantity
                 FROM gap_ils_cit_extract_t cit,
                      ris_isor_trandata_gtt gtt
                WHERE gtt.pack_ind  = 'N'
                  AND cit.status    = 'V'
                  AND cit.item      = gtt.item
                  AND cit.location  = gtt.location
               UNION ALL
               SELECT ENTITY_BOOK_TSF                        event_inv_txn_type,
                      'SHIPMENT'                             event_inv_sub_txn_type,
                      SYSDATE                                event_db_outbound_time,
                      gtt.ref_no_1                           event_attr1,
                      gtt.ref_no_2                           event_attr2,                                                             
                      gtt.location                      location, 
                      gtt.loc_type                      location_type, 
                      DECODE(gtt.loc_type,'W',
                                cit.legacy_loc,NULL)         physical_wh, 
                      CASE 
                      WHEN cit.chnl_id = 1
                      THEN 'RETAIL'
                      WHEN cit.chnl_id = 2
                      THEN 'ONLINE'
                      WHEN cit.chnl_id = 3
                      THEN 'OUTLET'
                      ELSE NULL
                      END                                    channel,
                      gtt.item                               global_item,
                      gtt.pack_ind                           packind,
                      cit.glbl_itm_sty_nbr                   item_parent,
                      cit.sty_clr_cd                         color,
                      cit.lgcy_size_cd                       size_cd,
                      cit.brd_id                             brand,
                      cit.mkt_id                             market,
                      REPLACE(cit.legacy_sku_id,'-','')      legacy_item,
                      to_char(xref.ppk_nbr)                  legacy_pack,
                      cit.lgcy_product_no                    legacy_cc,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units*pk.pack_qty
                      END                                    pack_comp_stock_on_hand,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units
                      END                                    in_transit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN  gtt.units*pk.pack_qty
                      END                                    pack_comp_intransit_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units
                      END                                    tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.from_loc = gtt.location)
                      THEN -1 * gtt.units*pk.pack_qty
                      END                                    pack_comp_tsf_alloc_reserved_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units
                      END                                    tsf_alloc_expected_quantity,
                      CASE
                      WHEN EXISTS(SELECT 1 
                                    FROM shipment sh
                                   WHERE sh.shipment  = gtt.ref_no_2
                                     AND sh.status_code = 'I' 
                                     AND sh.to_loc = gtt.location)
                      THEN (-1)*gtt.units*pk.pack_qty
                      END                                    pack_comp_tsf_alloc_expected_quantity 
                 FROM gap_ils_cit_extract_t cit,
                      packitem pk,
                      tppx_ppk_xref xref,
                      ris_isor_trandata_gtt gtt
                WHERE gtt.pack_ind    = 'Y'
                  AND gtt.ref_pack_no = pk.pack_no(+)
                  AND gtt.ref_pack_no = xref.pack_no(+)
                  AND cit.status      = 'V'
                  AND cit.item        = gtt.item
                  AND cit.location    = gtt.location)
         SELECT RIS_INV_SOR_ENVENTID_SEQ.NEXTVAL       event_id,
                event_inv_txn_type,
                event_inv_sub_txn_type,
                event_db_outbound_time,
                event_attr1,
                event_attr2,
                location, 
                location_type, 
                physical_wh, 
                channel,
                global_item,
                packind,
                item_parent,
                color,
                size_cd,
                brand,
                market,
                legacy_item,
                legacy_pack,
                legacy_cc,
                stock_on_hand,
                pack_comp_stock_on_hand,
                in_transit_quantity,
                pack_comp_intransit_quantity,
                tsf_alloc_reserved_quantity,
                pack_comp_tsf_alloc_reserved_quantity,
                tsf_alloc_expected_quantity,
                pack_comp_tsf_alloc_expected_quantity               
           FROM BOOKTSF_DATA; 
         --
      END IF;
      --
      RETURN TRUE;
      --
   EXCEPTION
   WHEN OTHERS
   THEN
      --
      O_error_message := SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                                             'ERROR: Unexpected Error'
                                             ||SQLERRM
                                             ||'; LAST PROCESSED: '
                                             ||L_mark,
                                             L_program_name,
                                             TO_CHAR (SQLCODE));

      RETURN FALSE;
      --
   END LOAD_GTT;
--------------------------------------------------------------------------------------------------
-- Function Name : FLUSH_GTT
-- Purpose       : Reads the data from stage tables and enrich and loads into ISOR final tables.
--------------------------------------------------------------------------------------------------
   FUNCTION FLUSH_GTT(O_error_message     IN OUT    VARCHAR2)
   RETURN BOOLEAN
   IS
      --
      L_program_name                VARCHAR2(1000)           := 'RIS_INV_SOR_HARNESS_SQL.FLUSH_GTT';
      L_mark                        VARCHAR2(1000);
      --L_cur_kafka_ob_stg            SYS_REFCURSOR;
      --L_sql_text                    VARCHAR2(320000);
      --
      L_process_chunk_size          NUMBER(10);
      L_sleep_time_secs             NUMBER(10);
      --
      CURSOR cur_get_cit_tran_data
      IS
        SELECT 
               RIS_ISOR_KAFKA_OBSTG_OBJ(
               gtt.event_id,                               --event_id, 
               gtt.event_inv_txn_type,                     --event_inv_txn_type, 
               gtt.event_inv_sub_txn_type,                 --event_inv_sub_txn_type, 
               gtt.event_db_outbound_time,                 --event_db_outbound_time, 
               gtt.event_attr1,                            --event_attr1, 
               gtt.event_attr2,                            --event_attr2, 
               gtt.event_attr3,                            --event_attr3, 
               gtt.event_attr4,                            --event_attr4, 
               gtt.event_attr5,                            --event_attr5, 
               gtt.event_attr6,                            --event_attr6, 
               gtt.event_attr7,                            --event_attr7, 
               gtt.event_attr8,                            --event_attr8, 
               gtt.event_attr9,                            --event_attr9, 
               gtt.event_attr10,                           --event_attr10,
               gtt.location,                               --location, 
               gtt.location_type,                          --location_type, 
               gtt.physical_wh,                            --physical_wh, 
               gtt.channel,                                --channel, 
               gtt.location_attribute1,                    --location_attribute1,
               gtt.location_attribute2,                    --location_attribute2,
               gtt.location_attribute3,                    --location_attribute3,
               gtt.location_attribute4,                    --location_attribute4,
               gtt.location_attribute5,                    --location_attribute5,
               gtt.global_item,                            --global_item, 
               gtt.packind,                                --packind, 
               gtt.item_parent,                            --item_parent, 
               gtt.color,                                  --color, 
               gtt.size_cd,                                --size_cd, 
               gtt.brand,                                  --brand, 
               gtt.market,                                 --market, 
               gtt.legacy_item,                            --legacy_item, 
               gtt.legacy_pack,                            --legacy_pack, 
               gtt.legacy_cc,                              --legacy_cc, 
               gtt.stock_on_hand,                          --stock_on_hand, 
               gtt.pack_comp_stock_on_hand,                --pack_comp_stock_on_hand, 
               gtt.in_transit_quantity,                    --in_transit_quantity,      
               gtt.pack_comp_intransit_quantity,           --pack_comp_intransit_quantity,
               gtt.tsf_alloc_reserved_quantity,            --tsf_alloc_reserved_quantity,
               gtt.pack_comp_tsf_alloc_reserved_quantity,  --pack_comp_tsf_alloc_reserved_quantity, 
               gtt.tsf_alloc_expected_quantity,            --tsf_alloc_expected_quantity,
               gtt.pack_comp_tsf_alloc_expected_quantity,  --pack_comp_tsf_alloc_expected_quantity
               gtt.non_sellable_quantity,                  --non_sellable_quantity
               gtt.customer_reserved_quantity,             --customer_reserved_quantity
               gtt.customer_backorder_quantity,            --customer_backorder_quantity
               gtt.on_order_qty                            --on_order_qty
               )
          FROM ris_isor_cit_trandata_gtt gtt;

      --  
   BEGIN
      --
      L_mark := 'Begin of FLUSH_GTT.';
      --
      L_mark := 'Open sales sleep config.';
      --
       OPEN cur_get_sleep_config;
      FETCH cur_get_sleep_config INTO L_process_chunk_size, L_sleep_time_secs ;
      CLOSE cur_get_sleep_config;
      --
      IF NVL(L_process_chunk_size,0) > 0
      THEN
         --
        L_mark := 'Open cur_get_cit_tran_data.';
         --
         OPEN cur_get_cit_tran_data;
         LOOP
         FETCH cur_get_cit_tran_data BULK COLLECT INTO L_tab_kafka_outbound_stg LIMIT L_process_chunk_size;
         EXIT WHEN L_tab_kafka_outbound_stg.COUNT = 0; 
            --
            L_mark := 'Call INSERT_KAFKA_STAGE.';
            --
            IF INSERT_KAFKA_STAGE(O_error_message) = FALSE
            THEN
               --
               RAISE PROGRAM_ERROR;
               --
            END IF;
            --
            SMT_UTIL.SLEEP(L_sleep_time_secs);
            --
          L_tab_kafka_outbound_stg := RIS_ISOR_KAFKA_OBSTG_TAB(); 
                --
         END LOOP;
         CLOSE cur_get_cit_tran_data; 
         --
      ELSE
         --
         L_mark := 'Open cur_get_cit_tran_data.';
         --
          OPEN cur_get_cit_tran_data;
         FETCH cur_get_cit_tran_data BULK COLLECT INTO L_tab_kafka_outbound_stg; 
         CLOSE cur_get_cit_tran_data;
         -- 
         L_mark := 'Call INSERT_KAFKA_STAGE.';
         
         --
         IF INSERT_KAFKA_STAGE(O_error_message) = FALSE
         THEN
            --
            RAISE PROGRAM_ERROR;
            --
         END IF;
         --
      END IF;
      --
      RETURN TRUE;
      --
   EXCEPTION
   WHEN OTHERS
   THEN
      --
      O_error_message := SQL_LIB.CREATE_MSG ('PACKAGE_ERROR',
                                             'ERROR: Unexpected Error'
                                             ||SQLERRM
                                             ||'; LAST PROCESSED: '
                                             ||L_mark,
                                             L_program_name,
                                             TO_CHAR (SQLCODE));

      RETURN FALSE;
      --
   END FLUSH_GTT;
-------------------------------------------------------------------------------------
--  Function Name: main
--  Purpose      :
-------------------------------------------------------------------------------------
   FUNCTION main(O_error_message      IN OUT  VARCHAR2,
                 I_chunk_id           IN      NUMBER,
                 I_entity_name        IN      VARCHAR2)
   RETURN BOOLEAN
   IS
      --
      L_program_name                VARCHAR2(1000)    := 'RIS_INV_SOR_HARNESS_SQL.MAIN';
      L_mark                        VARCHAR2(1000);
      L_fail_ind                    VARCHAR2(1) := 'N';
      
      CURSOR C_get_location IS
         SELECT key_1 location
           FROM ris_intf_thread
          WHERE program_name  = L_plsql_program_name
            AND status in ('N','E')
            AND chunk_id = I_chunk_id
            AND key_4 = I_entity_name;  

        
      --
      PROGRAM_ERROR            EXCEPTION;
      --
   BEGIN
      --
      L_mark := 'Validate the input parameters.';
      --
      IF I_entity_name IS NULL
      THEN
         --
         O_error_message := 'Error:: Expected input variable value is NULL.Please pass appropriate valid value.'||L_mark||L_program_name;
         RETURN FALSE;
         --
      END IF;
      --
      LP_entity_name  := I_entity_name;
      --
      L_mark := 'Get C_get_location';
      OPEN C_get_location;
      FETCH C_get_location BULK COLLECT INTO LP_location_tbl;
      CLOSE C_get_location;     

      IF LOAD_GTT(O_error_message) = FALSE
      THEN
         --
         RAISE PROGRAM_ERROR;
         --
      END IF;
      --
      IF FLUSH_GTT(O_error_message) = FALSE
      THEN
         --
         RAISE PROGRAM_ERROR;
         --
      END IF;
            
      L_mark := 'Update status to ris_intf_thread for chunk - '||I_chunk_id;
      --
      UPDATE ris_intf_thread intf
         SET status                 = STATUS_COMPLETE,
             error_message          = O_error_message,
             last_update_datetime   = SYSDATE
       WHERE program_name  = L_plsql_program_name
         AND status        IN (STATUS_ERROR,STATUS_NEW)
         AND chunk_id       = I_chunk_id;

      --
      O_error_message := ' Success For Chunk ID:' || I_chunk_id || ' Run ID:' || LP_run_id;   
      --
      RETURN TRUE;
      --
   EXCEPTION
   WHEN PROGRAM_ERROR
   THEN
      --
      O_error_message := SQL_LIB.CREATE_MSG('PACKAGE_ERROR',
                                            L_mark||':'||O_error_message,
                                            L_program_name,
                                            -20001);
      --
      ROLLBACK;
      --
      return FALSE;
      --
   WHEN OTHERS
   THEN
      --
      O_error_message := SQL_LIB.CREATE_MSG('PACKAGE_ERROR',
                                            L_mark||':'||SQLERRM,
                                            L_program_name,
                                            TO_CHAR (SQLCODE));
      --
      ROLLBACK;
      --
      RETURN FALSE;
      --
   END main;
--------------------------------------------------------------------------------------------------
END RIS_INV_SOR_HARNESS_SQL ;
/

SHOW ERRORS;    
