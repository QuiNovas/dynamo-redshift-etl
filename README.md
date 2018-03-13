# dynamo-redshift-etl
Lambda function that is notified on DynamoDB table changes and insert/updates a Redshift table. This is designed to be attached to a DynamoDB stream for updates.

# Environment Variables:
- **REDSHIFT_CLUSTER_IDENTIFIER** Optional, required if _**REDSHIFT_PASSWORD**_ is not present.
- **REDSHIFT_DB_NAME**
- **REDSHIFT_HOST**
- **REDSHIFT_PASSWORD** Optional, will get temporary credentials from Redshift using _**REDSHIFT_CLUSTER_INDENTIFIER**_ if not provided.
- **REDSHIFT_PORT** Optional, defaults to _5439_
- **REDSHIFT_USER**
- **DYNAMO_REDSHIFT_ETL** JSON with the following format. Recommend that you minify it. The values in the map _"fields"_ are in JSON Pointer format (RFC6901 - https://tools.ietf.org/html/rfc6901)
```
{
  "<dynamo-table-name>": {
    "table": "<redshift-table-name>",
    "primaryKey": "<key>",
    "fields": {
      "<key>": "/key1",
      "<field1>": "/val1",
      "<field2>": "/val2/subval3"
    }
  }
}
```



# Handler Method
function.handler


