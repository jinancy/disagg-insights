"""
Module for the constants used in the modules.
"""

OCIConstants = {'OS_HOSTNAME': "params.os_hostname",
                'PEM_FILE_PATH': "params.private_key",
                'COMPARTMENT_ID': "params.compartment_id",
                'TENANCY_ID': "params.tenancy_id",
                'USER_ID': "params.user_id",
                'FINGERPRINT': "params.fingerprint",
                'NAMESPACE': "params.namespace",
                'PROXY_URL': "params.proxy_url",
                'INVENTORY_BUCKET': "params.inventory_bucket",
                'BUCKET_PREFIX': "params.bucket_prefix",
                'REGION': "params.region",
                'TENANT_INVENTORY_BUCKET': "params.tenant_inventory_bucket"}

ColumnName = {'DATA_ID': "dataid",
              'LOCAL_15MIN': "local_15min",
              'GRID': "grid",
              'DAY_OF_MONTH': "dayofmonth",
              'DAY_OF_WEEK': "dayofweek",
              'DAY_OF_YEAR': "dayofyear",
              'DEW_POINT': "dewPoint",
              'IS_HOLIDAY': "is_holiday",
              'MONTH': "month",
              'TEMPERATURE': "temperature",
              'TOTAL': "total",
              'WEEK_OF_YEAR': "weekofyear"}

TENANT_NAME = 'tenant-name'
