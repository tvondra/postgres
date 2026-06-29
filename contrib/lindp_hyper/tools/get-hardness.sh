#!/usr/bin/env bash

# print ccp + timing
tail -n 100 pg.log | grep 'estimate_join_search_effort' | tail -n 1 | sed 's/.* aborted //' | awk '{print $1 " " $3 " " $5 " " $9}'
