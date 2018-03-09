# dynamo-redshift-etl
Lambda function that is notified on DynamoDB table changes and insert/updates a Redshift table

# Environment Variables:
- **REDSHIFT_HOST**
- **REDSHIFT_DB_NAME**
- **REDSHIFT_PORT** Optional, defaults to 5439
- **REDSHIFT_USER**
- **REDSHIFT_PASSWORD**
- **DYNAMO_REDSHIFT_ETL** JSON with the following format. Recommend that you minify it. The values in the map "fields" are in JSON Pointer format (RFC6901 - https://tools.ietf.org/html/rfc6901)
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


