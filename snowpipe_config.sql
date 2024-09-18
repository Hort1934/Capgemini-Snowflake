Stage Creation (AWS S3 Example):

CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://my-bucket-name/my-path-to-file/'
CREDENTIALS = (AWS_KEY_ID = 'my-key' AWS_SECRET_KEY = 'my-secret-key');


Snowpipe Definition:

CREATE OR REPLACE PIPE my_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw_airbnb_data
FROM @my_s3_stage
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
