import requests
from re import sub

from bs4 import BeautifulSoup
from requests_html import HTMLSession


class BnoNewsGateway:
    @staticmethod
    def fetch_raw():
        original_source = "https://bnonews.com/index.php/2020/02/the-latest-coronavirus-cases/"

        try:
            response = requests.get(original_source, timeout=60)
        except requests.Timeout:
            raise BnoNewsGatewayError("Timed out whilst fetching data from BnoNewsGateway")
        except requests.RequestException as re:
            raise BnoNewsGatewayError(f"Error occurred whilst fetching data from BnoNewsGateway ({str(re)})")

        if response.status_code != 200:
            raise BnoNewsGatewayError(
                "Non-200 status code returned from BnoNewsGateway (has the site address changed or is the site down?)"
            )

        soup = BeautifulSoup(response.text, features="html.parser")

        iframe_sources = soup.findAll("iframe")

        for iframe in iframe_sources:
            if "https://docs.google.com" in iframe.get("src"):
                real_data_source = sub(r"(\?gid).+", "/sheet?headers=false&gid=0", iframe.get("src"))
                break

        if not real_data_source:
            raise BnoNewsGatewayError("Couldn't find the source for the latest Coronavirus data")

        session = HTMLSession()

        try:
            r = session.get(real_data_source)
        except requests.Timeout:
            raise BnoNewsGatewayError("Timed out whilst fetching data from Google Docs")
        except requests.RequestException as re:
            raise BnoNewsGatewayError(f"Error occurred whilst fetching data from Google Docs ({str(re)})")

        return r.html.html


class BnoNewsGatewayError(Exception):
    pass
