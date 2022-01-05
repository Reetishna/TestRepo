CREATE OR REPLACE PACKAGE RISGAP.RIS_INV_SOR_HARNESS_SQL
AS
--------------------------------------------------------------------------------------------------
-- Package Name  :  RIS_INV_SOR_HARNESS_SQL
-- Purpose       : The purpose of this package is generate harness data for various functionalities. 
-- Author      Date              Ver      Desc                                 
-- -------    ---------          -----    ------------------------------------ 
-- HDC       22-Dec-2021          1.0      Initial version                      
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
-- Function Name : main
-- Purpose       : Loads the data into respective stage tables from ods_tran_data_history.
--------------------------------------------------------------------------------------------------

   FUNCTION GENERATE_CHUNKS(O_error_message   IN OUT   VARCHAR2,  
                            I_entity_name   IN         VARCHAR2)
   RETURN BOOLEAN;
--------------------------------------------------------------------------------------------------
-- Function Name : main
-- Purpose       : Loads the data into respective stage tables from ods_tran_data_history.
--------------------------------------------------------------------------------------------------

   FUNCTION MAIN(O_error_message   IN OUT   VARCHAR2,
                 I_chunk_id           IN      NUMBER,
                 I_entity_name     IN       VARCHAR2)
   RETURN BOOLEAN;
--------------------------------------------------------------------------------------------------
END RIS_INV_SOR_HARNESS_SQL ;
/
CREATE OR REPLACE PUBLIC SYNONYM RIS_INV_SOR_HARNESS_SQL  FOR RISGAP.RIS_INV_SOR_HARNESS_SQL ;
/
GRANT  EXECUTE ON RIS_INV_SOR_HARNESS_SQL  TO RISUSER;

SHOW ERRORS;


