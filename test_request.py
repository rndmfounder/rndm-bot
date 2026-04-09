import urllib.request

url = "https://api.telegram.org/bot8358029510:AAFv03CvLuhb0NiOQaWqEGjrskAiJrS7ty4/getMe"

try:
    with urllib.request.urlopen(url, timeout=20) as response:
        print(response.read().decode())
except Exception as e:
    print("ОШИБКА:", e)