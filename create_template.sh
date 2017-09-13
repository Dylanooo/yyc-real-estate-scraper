curl -XPUT 'localhost:9200/_template/yyc-homes-1?pretty' -H 'Content-Type: application/json' -d'
{
  "template": "yyc*",
  "mappings": {
    "listing": {
      "properties": {
        "year_built": {
          "type":   "date",
          "format": "yyyy"
        },
        "built": {
          "type":   "date",
          "format": "yyyy"
        },
        "location": {
            "type": "geo_point"
        }
      }
    }
  }
}
'