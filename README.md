```sh
curl localhost:5236/table/tony -H "Content-Type: application/json" -d '[{"a": 1, "b": 2}, {"a": 2, "b": 300}]'

curl 'localhost:5236/v1/$batch' -H "Content-Type: application/json" -d '{"requests": [{"id": "1", "body": {"query": "tony | where a == 2"}}]}'

# {"responses":[{"id":"1","status":200,"body":{"tables":[{"name":"PrimaryResult","columns":[{"name":"a","type":"int"},{"name":"b","type":"int"}],"rows":[[2,300]]}]}}]}
```
