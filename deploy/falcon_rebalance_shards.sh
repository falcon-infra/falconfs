#!/usr/bin/env bash

usage() 
{
    echo "Usage: $0 [id of source servers] [id of target servers] [cn ip] [cn port]"
    echo
    echo "Example: $0 1-3 1-2,4 127.0.0.1 55500"
}
if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

source_servers_id="$1"
target_servers_id="$2"
cn_ip="$3"
cn_port="$4"
temp_log_dir="temp_log_dir"
max_concurrency=32

expand_range_list() {
  local input="$1"
  local result=()

  IFS=',' read -r -a parts <<< "$input"
  for part in "${parts[@]}"; do
    if [[ "$part" =~ ^([0-9]+)-([0-9]+)$ ]]; then
      local start="${BASH_REMATCH[1]}"
      local end="${BASH_REMATCH[2]}"
      for ((i=start; i<=end; i++)); do
        result+=("$i")
      done
    else
      result+=("$part")
    fi
  done

  echo "${result[@]}"
}

pg_query_to_dict_array() {
  local sql="$1"
  local host="$2"
  local port="$3"
  local __outvar="$4"

  local output header rows_raw
  output=$(psql -d postgres -h "$host" -p "$port" -A -F $'\t' -P footer=off -c "$sql")
  if [[ $? -ne 0 ]]; then
    echo "psql query failed" >&2
    return 1
  fi

  header=$(head -n 1 <<< "$output")
  rows_raw=$(tail -n +2 <<< "$output")

  IFS=$'\t' read -r -a columns <<< "$header"

  local rows=()
  while IFS=$'\t' read -r -a fields; do
    declare -A row
    for ((i=0; i<${#columns[@]}; i++)); do
      row[${columns[$i]}]="${fields[$i]}"
    done
    rows+=("$(declare -p row | sed 's/^declare -A row=//')")
  done <<< "$rows_raw"

  # 将结果返回到调用者提供的变量名
  eval "$__outvar=(\"\${rows[@]}\")"
}

source_servers_id=($(expand_range_list "$source_servers_id"))
target_servers_id=($(expand_range_list "$target_servers_id"))
target_servers_count=${#target_servers_id[@]}

pg_query_to_dict_array \
  "select * from falcon_shard_table;" \
  "$cn_ip" \
  "$cn_port" \
  shards_info

total_shards_count=0
declare -A server_range_points
for shard_str in "${shards_info[@]}"; do
  eval "declare -A shard=$shard_str"

  server_id="${shard[server_id]}"
  range_point="${shard[range_point]}"

  if [[ " ${source_servers_id[*]} " =~ " ${server_id} " ]] || [[ " ${target_servers_id[*]} " =~ " ${server_id} " ]]; then
    server_range_points["$server_id"]+="$range_point "
    total_shards_count=$(( total_shards_count + 1 ))
  fi
done

target_shard_count_per_server_max=$(( (total_shards_count + target_servers_count - 1) / target_servers_count ))

shard_moves=("1,2,33")
candidate_servers_id=("${target_servers_id[@]}")
next_candidate_server_index=0
candidate_servers_id_size=${#candidate_servers_id[@]}
for source_server_id in "${source_servers_id[@]}"; do
  if [[ " ${target_servers_id[*]} " =~ " $source_server_id " ]]; then
    target_shard_count=$target_shard_count_per_server_max
  else
    target_shard_count=0
  fi
  read -ra source_server_range_points <<< "${server_range_points[$source_server_id]}"

  while [ "${#source_server_range_points[@]}" -gt $target_shard_count ]; do
    last_index=$((${#source_server_range_points[@]} - 1))
    range_point="${source_server_range_points[$last_index]}"

    # find target server
    target_server_id=""
    while [ -z "$target_server_id" ]; do
      if [ $candidate_servers_id_size -eq 0 ]; then
          echo "Error: No available target server"
          echo ${shard_moves[@]}
          exit 1
      fi

      candidate="${candidate_servers_id[$next_candidate_server_index]}"
      read -ra candidate_range_points <<< "${server_range_points[$candidate]}"
      current_len=${#candidate_range_points[@]}
      if [ "$current_len" -lt "$target_shard_count_per_server_max" ]; then
          target_server_id=$candidate
      else
          unset 'candidate_servers_id[$next_candidate_server_index]'
      fi

      ((next_candidate_server_index++))
      if [ $next_candidate_server_index -ge $candidate_servers_id_size ]; then
          next_candidate_server_index=0
          candidate_servers_id=("${candidate_servers_id[@]}")
          candidate_servers_id_size=${#candidate_servers_id[@]}
      fi
    done

    shard_moves+=("$source_server_id,$target_server_id,$range_point")

    unset 'source_server_range_points[$last_index]'
    server_range_points[$target_server_id]+="$range_point "
  done
done

if [ -e "$temp_log_dir" ]; then
  echo "Error: temp_log_dir='$temp_log_dir' already exists."
  exit 1
fi
mkdir $temp_log_dir

declare -A pids
declare -A logfiles

run_shard_move_task() 
{
  local i="$1"
  local triplet="${shard_moves[$i]}"
  IFS=',' read -r source_server_id target_server_id range_point <<< "$triplet"
  local logfile="$temp_log_dir/log_$i.log"

  (
    psql -d postgres -h "$cn_ip" -p "$cn_port" -A -F $'\t' -P footer=off \
         -c "SELECT falcon_move_shard($range_point, $target_server_id);"
  ) >"$logfile" 2>&1 &

  pids[$i]=$!
  logfiles[$i]=$logfile
}

wait_for_slot() 
{
  while true; do
    local running=0
    for pid in "${pids[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        ((running++))
      fi
    done

    if (( running < $max_concurrency )); then
      return
    fi

    wait -n 2>/dev/null || true
  done
}

task_count=${#shard_moves[@]}
for (( i=0; i<$task_count; i++ )); do
  wait_for_slot
  run_shard_move_task "$i"
done

for (( i=0; i<$task_count; i++ )); do
  wait "${pids[$i]}" || true
done

check_shard_move_task_success() 
{
  local logfile="$1"

  lines=$(wc -l <"$logfile")
  if (( lines < 2 )); then
    return 1
  fi

  local line1 line2
  line1=$(sed -n '1p' "$logfile")
  line2=$(sed -n '2p' "$logfile")

  if [[ "$line1" == "falcon_move_shard" ]] && [[ "$line2" == "0" ]]; then
      return 0
  else
      return 1
  fi
}

for (( i=0; i<$task_count; i++ )); do
  if check_shard_move_task_success "${logfiles[$i]}"; then
    rm ${logfiles[$i]}
  else
    echo "Task[$i] failed! The (source_server_id,target_server_id,range_point) is (${shard_moves[$i]}), please check ${logfiles[$i]}."
  fi
done

if [ -z "$(ls -A "$temp_log_dir")" ]; then
  echo "All task succeed!"
  rm -rf $temp_log_dir
fi