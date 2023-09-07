import requests


def weed_info():
    base_url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'
    try:
        result = requests.get(f"{base_url}/current.json")
        data = result.json()

    except Exception as e:
        print(e)

    finally:
        return data


weed_info()
