import urllib.request

url = "https://www.jamundi.gov.co/SiteAssets/Jamundi/assets/img/logofooter.png"
req = urllib.request.Request(
    url, 
    headers={'User-Agent': 'Mozilla/5.0'}
)

try:
    with urllib.request.urlopen(req) as response:
        with open("jamundi_report_banner_oficial.png", "wb") as f:
            f.write(response.read())
        print("Downloaded logofooter.png successfully as jamundi_report_banner_oficial.png")
except Exception as e:
    print(f"Error: {e}")
