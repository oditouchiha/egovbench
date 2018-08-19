#!/bin/bash

screen -X -S sparkstream_post quit
screen -X -S sparkstream_comment quit
screen -X -S twitterstream_post quit
screen -X -S twitterstream_comment quit
screen -X -S facebook_trigger quit
screen -X -S twitter_trigger quit
screen -X -S youtube_trigger quit
screen -X -S facebook_crawler quit
screen -X -S twitter_crawler quit
screen -X -S youtube_crawler quit

#screen -dm -S sparkstream_post bash -c 'source ~/egovbench/egov-env/bin/activate; ~/egovbench/scripts/egovbench_localsparksubmit.sh ${EGOVBENCH_PYTHON}/egovbench_sparkstream_post.py;'

#sleep 10

#screen -dm -S sparkstream_comment bash -c 'source ~/egovbench/egov-env/bin/activate; ~/egovbench/scripts/egovbench_localsparksubmit.sh ${EGOVBENCH_PYTHON}/egovbench_sparkstream_comment.py;'

#sleep 10

#screen -dm -S twitterstream_post bash -c 'source ~/egovbench/egov-env/bin/activate; python3 ${EGOVBENCH_PYTHON}/egovbench_twitterstream_post.py;'
#screen -dm -S twitterstream_comment bash -c 'source ~/egovbench/egov-env/bin/activate; python3 ${EGOVBENCH_PYTHON}/egovbench_twitterstream_comment.py;'
#screen -dm -S facebook_trigger bash -c 'source ~/egovbench/egov-env/bin/activate; python3 ${EGOVBENCH_PYTHON}/egovbench_facebooktrigger.py;' 
#screen -dm -S twitter_trigger bash -c 'source ~/egovbench/egov-env/bin/activate; python3 ${EGOVBENCH_PYTHON}/egovbench_twittertrigger.py;'
#screen -dm -S youtube_trigger bash -c 'source ~/egovbench/egov-env/bin/activate; python3 ${EGOVBENCH_PYTHON}/egovbench_youtubetrigger.py;'

screen -dm -S facebook_crawler bash -c 'source ~/egovbench/egov-env/bin/activate; python3 ${EGOVBENCH_PYTHON}/egovbench_facebookcrawler.py;'
#screen -dm -S twitter_crawler bash -c 'source ~/egovbench/egov-env/bin/activate; python3 ${EGOVBENCH_PYTHON}/egovbench_twittercrawler.py;'
screen -dm -S youtube_crawler bash -c 'source ~/egovbench/egov-env/bin/activate; python3 ${EGOVBENCH_PYTHON}/egovbench_youtubecrawler.py;'
