{
  "query": "SELECT count(*) AS value FROM trips WHERE status='completed' AND aql_time_filter(request_at, \"24 hours ago\", \"this quarter-hour\", America/New_York) GROUP BY aql_time_bucket_hour(request_at, \"\", America/New_York)"
}