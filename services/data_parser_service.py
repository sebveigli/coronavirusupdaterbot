import pandas as pd

from bs4 import BeautifulSoup


class DataParserService:
    @staticmethod
    def create_dataframe_from_bno_data(raw_data):
        """
        Parses the raw HTML gathered from BNO News and creates a DataFrame from it

        Params:
        raw_data (str) -> request.data from BNO

        Returns:
        DataFrame
        """
        COLUMNS = [
            "Location",
            "Cases",
            "Deaths",
            "Serious",
            "Critical",
            "Recovered",
            "Source",
        ]

        soup = BeautifulSoup(raw_data, features="html.parser")

        all_tr_rows = soup.find("tbody").findAll("tr")

        data = []
        data_active = False

        for tr_row in all_tr_rows:
            all_tds = tr_row.findAll("td")

            if not len(all_tds):
                continue
            elif all_tds[0].get_text() == "OTHER PLACES":
                data_active = True
                continue
            elif all_tds[0].get_text() == "TOTAL":
                data_active = False
                continue

            if data_active:
                data_row = [x.get_text() for x in all_tds[: len(COLUMNS) - 1]]

                # source_data = all_tds[len(COLUMNS) - 1].find("a")

                # if source_data:
                #     source = source_data["href"]
                # else:
                source = ""

                data_row.append(source)
                data.append(data_row)

        return pd.DataFrame(data, columns=COLUMNS)
