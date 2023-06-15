#! /bin/sh

curl "https://api.hypothes.is/api/search?user=public_reviews;tag=Summary" | \
  jq -r '.rows | .[] | "hypothesis:" + (.id) + " " + (.uri)' | \
  grep 10.1101 | \
  sed -e 's/ https.*content\// /' | \
  sed -e 's/v[0-9]*$/ b560187e-f2fb-4ff9-a861-a204f3fc0fb0/' | \
  xargs -n3 ./scripts/record-on-sciety.sh

