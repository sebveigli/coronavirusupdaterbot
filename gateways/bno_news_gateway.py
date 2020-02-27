import requests


class BnoNewsGateway:
    @staticmethod
    def fetch_raw():
        url = "https://bnonews.com/index.php/2020/02/the-latest-coronavirus-cases/"

        try:
            response = requests.get(url, timeout=60)
        except requests.Timeout:
            raise BnoNewsGatewayError("Timed out whilst fetching data from BnoNewsGateway")
        except request.RequestException as re:
            raise BnoNewsGatewayError(f"Error occurred whilst fetching data from BnoNewsGateway ({str(re)})")

        if response.status_code != 200:
            raise BnoNewsGatewayError(
                "Non-200 status code returned from BnoNewsGateway (has the site address changed or is the site down?)"
            )

        return response.text


class BnoNewsGatewayError(Exception):
    pass
