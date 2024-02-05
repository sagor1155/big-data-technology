#!/bin/bash
echo "Remove Existing Table and Create 'Stocks' Table (HBase)"
echo "disable 'stocks'" | hbase shell
echo "drop 'stocks'" | hbase shell
echo "create 'stocks', 'price', 'volumn'" | hbase shell

