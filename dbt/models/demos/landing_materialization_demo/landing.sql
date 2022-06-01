{{config(materialized='landing')}}

CREATE OR REPLACE TABLE {{this.database}}.{{this.schema}}.{{this.identifier}}

(
	RECORD_ID          INT     NOT NULL     COMMENT 'Unique ID of the record',
	"DATE"             DATE    NULL         COMMENT 'Date of record',
    RECORD_DESCRIPTION VARCHAR NULL         COMMENT 'Description of the record'
);