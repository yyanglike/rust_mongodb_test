#!/bin/bash

# Test data
TEST_DATA1='{
    "name": "John",
    "active": true,
    "address": {
        "street": "123 Main St",
        "city": "New York",
        "coordinates": {
            "lat": 40.7128,
            "lng": -74.0060
        }
    }
}'

TEST_DATA2='{
    "name": "Alice",
    "active": true,
    "address": {
        "street": "456 Second St",
        "city": "Los Angeles",
        "coordinates": {
            "lat": 34.0522,
            "lng": -118.2437
        }
    }
}'

# Store test data
echo "Storing test data..."
echo "Storing user1 data:"
curl -X POST -H "Content-Type: application/json" -d "$TEST_DATA1" http://localhost:8080/store/users/user1
echo -e "\nStoring user2 data:"
curl -X POST -H "Content-Type: application/json" -d "$TEST_DATA2" http://localhost:8080/store/users/user2

# Query all data
echo -e "\nQuerying all data:"
curl http://localhost:8080/query | jq

# Query specific user
echo -e "\nQuerying user1:"
curl http://localhost:8080/query/users/user1 | jq

# Search by name
echo -e "\nSearching for name 'John':"
curl http://localhost:8080/search/name/John | jq

# Search by city
echo -e "\nSearching for city 'New York':"
curl http://localhost:8080/search/address.city/New%20York | jq

# Search by coordinates
echo -e "\nSearching for latitude 40.7128:"
curl http://localhost:8080/search/address.coordinates.lat/40.7128 | jq

# Cleanup old data
echo -e "\nTesting cleanup functionality..."
curl -X DELETE http://localhost:8080/cleanup

# Verify cleanup
echo -e "\nQuerying after cleanup:"
# curl http://localhost:8080/query | jq
