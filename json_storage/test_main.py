import requests

BASE_URL = "http://127.0.0.1:8000"

def test_insert_json():
    url = f"{BASE_URL}/root/first_level"
    data = {
        "name": "John",
        "age": 30,
        "city": "Beijing",
        "details": {
            "hobby": "reading",
            "job": "engineer"
        }
    }
    response = requests.post(url, json=data)
    print("Response Status Code:", response.status_code)
    print("Response Content:", response.content)
    assert response.status_code == 200
    print("Insert JSON Response:", response.json())

def test_get_all_json():
    url = f"{BASE_URL}/root/first_level"
    response = requests.get(url)
    assert response.status_code == 200
    print("Get All JSON Response:", response.json())

def test_get_json_by_id():
    url = f"{BASE_URL}/root/first_level/1"
    response = requests.get(url)
    assert response.status_code == 200
    print("Get JSON by ID Response:", response.json())

def test_update_json():
    url = f"{BASE_URL}/root/first_level/1"
    data = {
        "name": "Jane",
        "age": 25,
        "city": "Shanghai",
        "details": {
            "hobby": "writing",
            "job": "designer"
        }
    }
    response = requests.put(url, json=data)
    assert response.status_code == 200
    print("Update JSON Response:", response.json())

def test_delete_json():
    url = f"{BASE_URL}/root/first_level/1"
    response = requests.delete(url)
    assert response.status_code == 200
    print("Delete JSON Response:", response.json())

if __name__ == "__main__":
    test_insert_json()
    test_get_all_json()
    test_get_json_by_id()
    test_update_json()
    test_delete_json()