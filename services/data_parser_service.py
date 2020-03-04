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
        COLUMNS = ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"]

        soup = BeautifulSoup(raw_data, features="html.parser")

        all_tr_rows = soup.find("tbody").findAll("tr")

        china_data = []
        international_data = []

        china_active = False
        international_active = False

        for idx, tr_row in enumerate(all_tr_rows):
            all_tds = tr_row.findAll("td")

            if len(all_tds) != 7:
                continue
            elif all_tds[0].get_text() == "MAINLAND CHINA":
                china_active = True
                continue
            elif all_tds[0].get_text() == "CHINA TOTAL":
                china_active = False
                continue
            elif all_tds[0].get_text() == "OTHER PLACES":
                international_active = True
                continue
            elif all_tds[0].get_text() == "TOTAL":
                international_active = False
                continue

            if china_active:
                china_data.append([x.get_text() for x in all_tds])
            elif international_active:
                international_data.append([x.get_text() for x in all_tds])

        if region == "international":
            return pd.DataFrame(international_data, columns=COLUMNS)
        else:
            return pd.DataFrame(china_data, columns=COLUMNS)
