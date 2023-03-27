#!/bin/bash
echo "AWS user: "
read user
echo "AWS seret: "
read secret

docker run -e AWS_ACCESS_KEY_ID=$user -e AWS_SECRET_ACCESS_KEY=$secret -e AWS_REGION=eu-west-1 -e AWS_DEFAULT_REGION=eu-west-1 --name spark_container spark_container