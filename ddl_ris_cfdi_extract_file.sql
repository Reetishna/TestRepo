
CREATE TABLE RIS_CFDI_EXTRACT_FILE
   (CONTROL_NO              NUMBER(20),
    STORE                   NUMBER(10,0),
    DAY                     NUMBER(3,0),
    REGISTER                VARCHAR2(5),
    TRAN_DATETIME           TIMESTAMP,
    TRAN_NO                 NUMBER(10),
    TRAN_SEQ_NO             NUMBER(20,0),
    FILE_TYPE               VARCHAR2(10),
    REV_NO                  NUMBER(3,0),
    RECORD_TYPE             VARCHAR2(10),
    SEQ_NO                  NUMBER(20,0),
    ITEM_SEQ_NO             NUMBER(20),
    QTY                     NUMBER(12,4), 
    VALUE                   NUMBER(20,8),
    UNIT_RETAIL             NUMBER(20,4),
    TOTAL_IGTAX_AMT         NUMBER(12,4),
    UNIT_DISCOUNT_AMT       NUMBER(12,4), 
    RECORD_TEXT             VARCHAR2(2000),
    run_id                  NUMBER(20),
    create_datetime         DATE,
    create_id               VARCHAR2(30),
    last_update_datetime    DATE, 
    last_update_id          VARCHAR2(30)
   );

CREATE OR REPLACE PUBLIC SYNONYM RIS_CFDI_EXTRACT_FILE FOR RIS_CFDI_EXTRACT_FILE;


