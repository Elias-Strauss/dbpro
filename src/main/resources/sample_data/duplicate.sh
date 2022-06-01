#!/bin/bash
limit=1500
for ((i=0; i<limit; i++)); do cat users.csv >> big/users.csv; done
