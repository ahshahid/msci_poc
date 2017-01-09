
-- DROP TABLE IF ALREADY EXISTS --
DROP INDEX IF EXISTS BRF_CON_INST_NAME;
DROP INDEX IF EXISTS BTS_IR_OBS_ID;
DROP TABLE IF EXISTS BTS_IR_OBS;
DROP TABLE IF EXISTS INST_NAME_CORRECTION;
DROP TABLE IF EXISTS BTS_IR_OBS_CORRECTION;
DROP TABLE IF EXISTS BRF_VAL_TYPE;
DROP TABLE IF EXISTS BRF_IR_NODE;
DROP TABLE IF EXISTS BRF_IR;
DROP TABLE IF EXISTS BRF_CON_INST;

----- CREATE ROW TABLEs -----

CREATE TABLE BRF_CON_INST (
ID  Integer not null primary key,
INST_TYPE INTEGER not null,
NAME VARCHAR(130) not null,
DESCRIPTION VARCHAR(255) not null,
DATASET VARCHAR(50) not null,
START_DATE TIMESTAMP,
VALID_RNG_START TIMESTAMP,
VALID_RNG_END TIMESTAMP,
TRANS_RNG_START TIMESTAMP ,
TRANS_RNG_END TIMESTAMP
)  USING row OPTIONS(partition_by 'ID' ,buckets '79') ;

CREATE INDEX BRF_CON_INST_NAME  ON BRF_CON_INST(NAME) ;

CREATE TABLE BRF_IR (
INST_ID  INTEGER not null primary key,
CURRENCY VARCHAR(30) not null,
PAY_FREQ  Integer ,
VALID_RNG_START TIMESTAMP,
VALID_RNG_END TIMESTAMP,
TRANS_RNG_START TIMESTAMP ,
TRANS_RNG_END TIMESTAMP,
CONSTRAINT fk_id FOREIGN KEY (INST_ID) REFERENCES BRF_CON_INST(ID)
)  USING row OPTIONS(partition_by 'INST_ID', colocate_with 'BRF_CON_INST' ,buckets '79') ;



CREATE TABLE BRF_IR_NODE (
INST_ID  Integer not null ,
NODE_ID Integer not null,
MATURITY  VARCHAR(10) ,
VALID_RNG_START TIMESTAMP,
VALID_RNG_END TIMESTAMP,
TRANS_RNG_START TIMESTAMP  ,
TRANS_RNG_END TIMESTAMP  ,
CONSTRAINT fk_inst_id FOREIGN KEY (INST_ID) REFERENCES BRF_IR(INST_ID)
) USING row OPTIONS(partition_by 'INST_ID', colocate_with 'BRF_IR' ,buckets '79') ;



CREATE TABLE BRF_VAL_TYPE (
VAL_TYPE Integer not null,
DESCRIPTION  VARCHAR(60)
) USING row OPTIONS() ;


-------------------- CREATE COLUMN TABLEs------------

CREATE TABLE BTS_IR_OBS (
NODE_ID  INTEGER ,
OBS_DATE TIMESTAMP,
VAL_TYPE  Integer ,
VAL numeric(24, 12),
TRANS_RNG_START TIMESTAMP  not null
)  USING row OPTIONS(partition_by 'NODE_ID', colocate_with 'BRF_CON_INST', buckets '79');

CREATE INDEX BTS_IR_OBS_ID ON BTS_IR_OBS(NODE_ID, val_type)