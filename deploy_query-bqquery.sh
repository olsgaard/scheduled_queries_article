. ./config_rc.sh

bq query \
    --use_legacy_sql=false \
    --destination_table=$dataset_name.$table_name \
    --display_name='Weekly Lightning' \
    --append_table \
    --schedule='every sun 00:00' \
    `cat weekly.sql` \
| tee /dev/tty | cut -d "'" -f 2 > resource_name.txt

# Show the newly created resource
bq show --format prettyjson --transfer_config `cat resource_name.txt`