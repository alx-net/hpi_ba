#bin/bash

nohup /usr/share/logstash/bin/logstash --path.settings /home/ubuntu/logstash_config/postgres/ &> /home/ubuntu/logstash_config/logstash.log &