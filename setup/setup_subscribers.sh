#!/bin/bash
# Automate creation of subscriber(s) on current host
while getopts c:t:m:i:b:s:p:d: flag
do
    case "${flag}" in
        c) count=${OPTARG};;
        t) topics+=(${OPTARG});;
        p) producers+=(${OPTARG});;
        m) max_event_count=${OPTARG};;
        i) indefinite=${OPTARG};;
        d) verbose=${OPTARG};;
    esac
done

# Build -t TOPIC -t TOPIC ... list
topic_arguments=""
for topic in "${topics[@]}"; do
  topic_arguments+="-t ${topic} "
done

# Build -p PRODUCER_IP:PORT -p PRODUCER_IP:PORT ... list
producer_arguments=""
for producer in "${producers[@]}"; do
  producer_arguments+="-p ${producer} "
done
# Defaults
if [ -z "$count" ] ; then
    count=1
fi
if [ -z "$verbose" ] ; then
    verbose=""
else
    verbose=" -v "
fi


echo "Creating $count subscribers..."
sleep 1

if [ -z "$max_event_count" ] ; then
    # Use indefinite publishing if -i is provided or if neither -i nor -m COUNT provided
    # Use max event count
    echo "python ../driver.py --subscriber $count $topic_arguments $producer_arguments --indefinite $verbose"
    python ../driver.py --subscriber $count $topic_arguments $producer_arguments --indefinite $verbose
else
    # Use max event count
    echo "python ../driver.py --subscriber $count $topic_arguments $producer_arguments --max_event_count $max_event_count $verbose"
    python ../driver.py --subscriber $count $topic_arguments $producer_arguments --max_event_count $max_event_count $verbose
fi

echo "$count subscribers created on this host"
exit 0