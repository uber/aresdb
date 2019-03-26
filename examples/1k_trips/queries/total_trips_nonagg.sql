{
  "query": "SELECT * FROM trips WHERE status='completed' AND aql_time_filter(request_at, \"24 hours ago\", \"this quarter-hour\", America/New_York)"
}