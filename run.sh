#!/bin/bash

#kill -9 `pidof node`

nohup supervisor -- app.js > 11747.log &
