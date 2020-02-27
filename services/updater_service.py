import asyncio
import logging
import pandas as pd

from gateways.bno_news_gateway import BnoNewsGatewayError

from texttable import Texttable


class UpdaterService:
    def __init__(
        self, bno_news_gateway, data_parser_service, region, update_interval, discord_channel_id, output, logger=None,
    ):
        self.bno_news_gateway = bno_news_gateway
        self.data_parser_service = data_parser_service
        self.region = region
        self.update_interval = update_interval
        self.discord_channel_id = discord_channel_id
        self.output = output
        self.previous_data = pd.DataFrame
        self.logger = logger if logger else logging.getLogger(__name__)

    async def update_loop(self, discord_client):
        self.logger.info("Coronavirus Updater Initialised")

        while not discord_client.is_closed():
            self.logger.info("Fetching the latest Coronavirus statistics")

            try:
                latest_data = self.bno_news_gateway.fetch_raw()
            except BnoNewsGatewayError as bnge:
                self.logger.critical(f"Failed to fetch the latest virus data - {str(bnge)}")
                self.logger.info("Trying again in 60 seconds...")
                await asyncio.sleep(60)
                continue

            self.logger.debug("Data fetched successfully. Parsing...")

            if self.region in ["china", "international"]:
                data = self.data_parser_service.create_dataframe_from_bno_data(latest_data, self.region)
            else:
                china_data = self.data_parser_service.create_dataframe_from_bno_data(latest_data, "china")
                international_data = self.data_parser_service.create_dataframe_from_bno_data(
                    latest_data, "international"
                )

                data = china_data.append(international_data, ignore_index=True)

            self.logger.debug("Data parsed successfully")

            if not self.previous_data.empty:
                self.logger.debug("Checking against previous data")

                data_diff = pd.concat([self.previous_data, data]).drop_duplicates(keep=False)

                if not data_diff.empty:
                    self.logger.debug("Data has changed. Creating and sending messages")
                    update_messages = self._make_update_message(data_diff)

                    for update in update_messages:
                        await discord_client.get_channel(self.discord_channel_id).send(update)
                else:
                    self.logger.debug("No changes in data - sleeping")
            else:
                self.logger.info("Previous data is empty - not comparing. Updating values")

            self.previous_data = data

            await asyncio.sleep(self.update_interval)

    def _make_update_message(self, data):
        if self.output == "table":
            return self._make_table_update(data)
        else:
            return self._make_text_update(data)

    def _make_table_update(self, data):
        table = Texttable()

        table.set_cols_align(["c"] * len(data.columns))
        table.set_cols_valign(["m"] * len(data.columns))

        parsed_data = self._collect_differences(data)

        new_data = [data.columns.tolist()]

        for index, row in parsed_data.iterrows():
            (location, cases_before, cases_after, deaths_before, deaths_after, notes,) = row

            cases_diff = int(cases_after) - int(cases_before)
            deaths_diff = int(deaths_after) - int(deaths_before)

            if cases_diff > 0:
                cases = "".join([cases_after, " (+", str(cases_diff), ")"])
            elif cases_diff < 0:
                cases = "".join([cases_after, " (", str(cases_diff), ")"])
            else:
                cases = cases_after

            if deaths_diff > 0:
                deaths = "".join([deaths_after, " (+", str(deaths_diff), ")"])
            elif deaths_diff < 0:
                deaths = "".join([deaths_after, " (", str(deaths_diff), ")"])
            else:
                deaths = deaths_after

            new_data.append([location, cases, deaths, notes])

        table.add_rows(new_data)

        message_cache = []
        all_messages = []

        table_split = table.draw().split("\n")

        while table_split:
            message = table_split.pop(0) + "\n"
            message_cache.append([message])

            total_message = "".join(m[0] for m in message_cache)

            if len(total_message) > 1700 or not table_split:
                all_messages.append(f"```{total_message[:-1]}```")
                message_cache = []

        return all_messages

    def _make_text_update(self, data):
        TEXT_TEMPLATE = {
            "cases_up": "{count} new case(s) identified in **{location}**, total case(s) now are {current}{notes}",
            "cases_down": "{count} incorrectly identified case(s) in **{location}**, total case(s) now are {current}{notes}",
            "deaths_up": "{count} new death(s) recorded in **{location}**, total death(s) now are {current}",
            "deaths_down": "{count} incorrectly identified death(s) in **{location}**, total death(s) now are {current}",
        }

        parsed_data = self._collect_differences(data)

        message_store = []

        for index, row in parsed_data.iterrows():
            (location, cases_before, cases_after, deaths_before, deaths_after, notes,) = row

            cases_diff = int(cases_after) - int(cases_before)
            deaths_diff = int(deaths_after) - int(deaths_before)

            if cases_diff > 0:
                message_store.append(
                    TEXT_TEMPLATE["cases_up"].format(
                        count=cases_diff, location=location, current=cases_after, notes=f" ({notes})" if notes else "",
                    )
                ),
            elif cases_diff < 0:
                message_store.append(
                    TEXT_TEMPLATE["cases_down"].format(
                        count=abs(cases_diff),
                        location=location,
                        current=cases_after,
                        notes=f" ({notes})" if notes else "",
                    )
                ),

            if deaths_diff > 0:
                message_store.append(
                    TEXT_TEMPLATE["deaths_up"].format(count=deaths_diff, location=location, current=deaths_after,)
                ),
            elif deaths_diff < 0:
                message_store.append(
                    TEXT_TEMPLATE["deaths_down"].format(
                        count=abs(deaths_diff), location=location, current=deaths_after,
                    )
                ),

        message_cache = []
        all_messages = []

        while message_store:
            message = message_store.pop(0) + "\n"
            message_cache.append([message])

            total_message = "".join(m[0] for m in message_cache)

            if len(total_message) > 1700 or not message_store:
                all_messages.append(total_message[:-1])
                message_cache = []

        return all_messages

    def _collect_differences(self, data):
        columns = [
            "location",
            "cases_before",
            "cases_after",
            "deaths_before",
            "deaths_after",
            "notes",
        ]
        parsed_data = []
        changed_locations = data.Location.unique()

        for location in changed_locations:
            location_data = data.loc[data["Location"] == location]

            if len(location_data) == 1:
                cases_before = "0"
                deaths_before = "0"
            else:
                cases_before = location_data.iloc[0]["Cases"]
                deaths_before = location_data.iloc[0]["Deaths"]

            cases_after = location_data.iloc[-1]["Cases"]
            deaths_after = location_data.iloc[-1]["Deaths"]

            notes = location_data.iloc[-1]["Notes"]

            parsed_data.append(
                [location, cases_before, cases_after, deaths_before, deaths_after, notes,]
            )

        return pd.DataFrame(parsed_data, columns=columns)
