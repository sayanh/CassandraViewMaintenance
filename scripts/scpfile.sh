#!/bin/bash
FILE=$1
DEST=$2

echo "------------ Transferring "$FILE " to VM1"
scp $1 anarchy@vm1:$DEST

echo "------------ Transferring "$FILE " to VM2"
scp $1 anarchy@vm2:$DEST


