--	================Velocity==============
--	Description: - This table to keep the track of when data was last extracted/ loaded
--	======================================
CREATE TABLE [etl].[watermark] (
    [timestamp]   DATETIME NOT NULL,	-- timestamp (parent audit timestamp) in format yyyyMMddHHmmssfffffff when the watermark is recorded via an execution
    [object_name] VARCHAR (255) NOT NULL,	-- the name of table, view of the watermark
    [key_1]       VARCHAR (255) NOT NULL,	-- watermark value, it can be the max of the delta column in the source system, it can be sysdate of the source when data extracted
    [key_1_desc]  VARCHAR (255) NULL,		-- keep data type of the delta column (datetime, date or number)
    [key_2]       VARCHAR (255) NULL,		
    [key_2_desc]  VARCHAR (255) NULL,
    [key_3]       VARCHAR (255) NULL,
    [key_3_desc]  VARCHAR (255) NULL,
    [key_4]       VARCHAR (255) NULL,
    [key_4_desc]  VARCHAR (255) NULL
);