#!/bin/bash
url="http://172.31.4.239:5000"
gpu_list=("rtx3060" "rtx3070" "rtx3080" "rtx3090" "rx6700")

date=$(date '+%Y-%m-%d %H:%M:%S')
function get_sales(){
    gpu="$1"
    sales=$(curl -s "$url/$gpu")
    echo "$gpu : $sales">> sales.txt
}
echo "$date" >> sales.txt
for gpu in "${gpu_list[@]}"
do
 get_sales "$gpu" 
done
