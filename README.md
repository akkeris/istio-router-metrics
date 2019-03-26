curl -G -d "start=3h-ago" -d "m=sum:5m-avg:router.service.ms{fqdn=event-perf-prd.alamoapp.octanner.io}" http://10.84.25.51:4242/api/query  | jq '.'


http://grafana.dev.octanner.net/dashboard/db/alamo-router-metrics


curl -G -d "start=3h-ago" -d "m=sum:1m-sum:router.status.200{fqdn=event-perf-prd.alamoapp.octanner.io}" http://10.84.25.51:4242/api/query  | jq '.'



curl -G -d "m=*{fqdn=event-perf-prd.alamoapp.octanner.io}" http://10.84.25.51:4242/api/search/lookup | jq '.'




