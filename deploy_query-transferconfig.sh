. ./config_rc.sh

query=`cat $sql_query_fn`
json='{
"destination_table_name_template" : "'$table_name'",
"write_disposition": "WRITE_APPEND",
"partitioning_field": "last_week_from",
"query": "'`echo $query`'"
}'

bq mk --transfer_config \
    --target_dataset=$dataset_name \
    --display_name='Weekly Lightning' \
    --schedule='every sunday 00:00' \
    --data_source=scheduled_query \
    --schedule_start_time="`date -u +%Y-%m-%dT%TZ`" \
    --params="$json" \
| tee /dev/tty | cut -d "'" -f 2 > resource_name.txt

# Show the newly created resource
bq show --format prettyjson --transfer_config `cat resource_name.txt`