import requests

url = 'https://google.com'

resp = requests.get(url)

print(resp.content)