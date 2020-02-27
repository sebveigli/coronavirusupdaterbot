import pandas as pd

from bs4 import BeautifulSoup


class DataParserService:
    @staticmethod
    def create_dataframe_from_bno_data(raw_data, region="international"):
        """
        Parses the raw HTML gathered from BNO News and creates a DataFrame from it

        Params:
        raw_data (str) -> request.data from BNO
        region (str, default: international) -> "international" for the whole world, "china" for China only

        Returns:
        DataFrame
        """
        COLUMNS = ["Location", "Cases", "Deaths", "Notes"]

        soup = BeautifulSoup(raw_data, features="html.parser")

        if region == "international":
            _class = "wp-block-table is-style-regular"
        else:
            _class = "wp-block-table aligncenter is-style-stripes"

        all_data = soup.find("table", class_=_class).find("tbody").findAll("tr")

        data_header = [data.get_text() for data in all_data[0].findAll("td")][:-1]
        data_values = all_data[1:-1]

        parsed_data = []

        for d in data_values:
            parsed_data.append([data.get_text() for data in d.findAll("td")][:-1])

        return pd.DataFrame(parsed_data, columns=COLUMNS)
