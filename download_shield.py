import urllib.request

def download_shield():
    url = "https://upload.wikimedia.org/wikipedia/commons/thumb/c/cb/Escudo_de_Jamund%C3%AD.svg/512px-Escudo_de_Jamund%C3%AD.svg.png"
    
    req = urllib.request.Request(
        url, 
        headers={'User-Agent': 'Mozilla/5.0'}
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            with open("jamundi_report_banner_oficial.png", "wb") as f:
                f.write(response.read())
        print("Shield downloaded successfully.")
    except Exception as e:
        print(f"Failed to download shield: {e}")

if __name__ == "__main__":
    download_shield()
