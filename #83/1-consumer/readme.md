

#### create virtual enviroment python
```sh
virtualenv consumer-venv
source ./consumer-venv/bin/activate
```


#### install the libraries

```sh
pip install -r requirements.txt
```



#### consumer group command line

```sh

kubectl exec edh-kafka-0 -c kafka -i -t -- \
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list


kubectl exec edh-kafka-0 -c kafka -i -t -- \
bin/kafka-consumer-groups.sh  --bootstrap-server localhost:9092 --describe --group python-app-consumer-live-83-16-37


```