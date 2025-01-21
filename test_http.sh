#!/bin/bash

# Store multiple user records
curl -X POST -H "Content-Type: application/json" -d '{
    "user": {
        "name": "John",
        "active": true,
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "location": {
                "coordinates": {
                    "latitude": 40.7128,
                    "longitude": -74.0060
                }
            },
            "tags": ["home", "primary"]
        }
    }
}' http://localhost:8080/store

curl -X POST -H "Content-Type: application/json" -d '{
    "user": {
        "name": "Alice",
        "active": true,
        "address": {
            "street": "456 Second St",
            "city": "Los Angeles",
            "location": {
                "coordinates": {
                    "latitude": 34.0522,
                    "longitude": -118.2437
                }
            },
            "tags": ["work", "secondary"]
        }
    }
}' http://localhost:8080/store

curl -X POST -H "Content-Type: application/json" -d '{
    "user": {
        "name": "Bob",
        "active": false,
        "address": {
            "street": "789 Third Ave",
            "city": "Chicago",
            "location": {
                "coordinates": {
                    "latitude": 41.8781,
                    "longitude": -87.6298
                }
            },
            "tags": ["vacation", "temporary"]
        }
    }
}' http://localhost:8080/store

# Query all users
echo -e "\nAll users:"
curl http://localhost:8080/query

# Query by name
echo -e "\nSearching for name 'John':"
curl http://localhost:8080/query/name/John

echo -e "\nSearching for name 'Alice':"
curl http://localhost:8080/query/name/Alice

echo -e "\nSearching for name 'Bob':"
curl http://localhost:8080/query/name/Bob

# Query by city
echo -e "\nSearching for city 'New York':"
curl http://localhost:8080/query/address.city/New%20York

# Query by active status
echo -e "\nSearching for active users:"
curl http://localhost:8080/query/active/true

# Query by tag
echo -e "\nSearching for users with 'home' tag:"
curl http://localhost:8080/query/address.tags/home

# Cleanup old data
echo -e "\nTesting cleanup functionality..."
curl -X DELETE http://localhost:8080/cleanup

# Query after cleanup
echo -e "\nQuerying after cleanup:"
curl http://localhost:8080/query
