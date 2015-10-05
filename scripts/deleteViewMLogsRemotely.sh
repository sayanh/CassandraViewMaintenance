#!/bin/bash

echo "------------------------------------------------------"
echo "-----------------Working in VM1 ----------------------"
echo "------------------------------------------------------"

#ssh anarchy@vm1 bash -c 'rm ~/cassandraviewmaintenance/logs/viewMaintenceCommitLogsv2.log'

ssh anarchy@vm1 'rm -rf /home/anarchy/cassandraviewmaintenance/logs/*;rm ~/cassandraviewmaintenance/data/viewmaintenance_status.txt'

if [ $? -eq 0  ]; then
  echo "Successfully removed logs in VM1"
else
  echo "Error in removing logs in VM1"
fi


echo "------------------------------------------------------"
echo "-----------------Working in VM2 ----------------------"
echo "------------------------------------------------------"

ssh anarchy@vm2 'rm -rf /home/anarchy/cassandraviewmaintenance/logs/*;rm ~/cassandraviewmaintenance/data/viewmaintenance_status.txt'

if [ $? -eq 0  ]; then
  echo "Successfully removed logs in VM2"
else
  echo "Error in removing logs in VM2"
fi

