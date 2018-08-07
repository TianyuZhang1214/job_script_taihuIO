thread_count=2
startid=42500000
file_name="IOmode_${startid}"
table="JOB_log"
mkdir ../../results_job_data/collect_data/$file_name
nohup python save_IOmode_sql.py $file_name $thread_count $startid $table > ../../results_job_data/collect_data/$file_name/collect_$file_name.log 2>&1 &
#python save_IOmode_sql.py $file_name $thread_count $startid $table > ../../results_job_data/collect_data/$file_name/collect_$file_name.log 2>&1
