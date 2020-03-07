import asyncio
import datetime
import discord
import logging
import pandas as pd

from config import embed as embed_config
from gateways.bno_news_gateway import BnoNewsGatewayError
from utils.data import remove_non_integers_from_string

from texttable import Texttable


class UpdaterService:
    def __init__(
        self, bno_news_gateway, data_parser_service, update_interval, discord_channel_id, output, logger=None,
    ):
        self.bno_news_gateway = bno_news_gateway
        self.data_parser_service = data_parser_service
        self.update_interval = update_interval
        self.discord_channel_id = discord_channel_id
        self.output = output
        self.previous_data = pd.DataFrame
        self.logger = logger if logger else logging.getLogger(__name__)

    async def update_loop(self, discord_client):
        self.logger.info("Coronavirus Updater Initialised")

        while not discord_client.is_closed():
            timestamp = datetime.datetime.utcnow()

            self.logger.info("Fetching the latest Coronavirus statistics")

            try:
                latest_data = self.bno_news_gateway.fetch_raw()
            except BnoNewsGatewayError as bnge:
                self.logger.critical(f"Failed to fetch the latest virus data - {str(bnge)}")
                self.logger.info("Trying again in 20 seconds...")
                await asyncio.sleep(20)
                continue

            self.logger.debug("Data fetched successfully. Parsing...")

            data = self.data_parser_service.create_dataframe_from_bno_data(latest_data)

            self.logger.debug("Data parsed successfully")

            if not self.previous_data.empty:
                self.logger.debug("Checking against previous data")

                data_diff = pd.concat([self.previous_data, data]).drop_duplicates(keep=False)

                if not data_diff.empty:
                    self.logger.debug("Data has changed. Creating and sending messages")
                    update_messages = self._make_update_message(data_diff, timestamp)

                    for update in update_messages:
                        if type(update) == discord.Embed:
                            await discord_client.get_channel(self.discord_channel_id).send(embed=update)
                        else:
                            await discord_client.get_channel(self.discord_channel_id).send(update)
                else:
                    self.logger.debug("No changes in data - sleeping")
            else:
                self.logger.info("Previous data is empty - not comparing. Updating values")

            self.previous_data = data

            await asyncio.sleep(self.update_interval)

    def _make_update_message(self, data, timestamp):
        if self.output == "table":
            return self._make_table_update(data)
        elif self.output == "text":
            return self._make_text_update(data)
        elif self.output == "embed":
            return self._make_embed_update(data, timestamp)

    def _make_table_update(self, data):
        table = Texttable()

        table.set_cols_align(["c"] * len(data.columns))
        table.set_cols_valign(["m"] * len(data.columns))

        parsed_data = self._collect_differences(data)

        new_data = [data.columns.tolist()]

        for _, row in parsed_data.iterrows():
            (
                location,
                cases_before,
                cases_after,
                deaths_before,
                deaths_after,
                serious_before,
                serious_after,
                critical_before,
                critical_after,
                recovered_before,
                recovered_after,
                source,
            ) = row

            cases_diff = int(cases_after) - int(cases_before)
            deaths_diff = int(deaths_after) - int(deaths_before)
            serious_diff = int(serious_after) - int(serious_before)
            critical_diff = int(critical_after) - int(critical_before)
            recovered_diff = int(recovered_after) - int(recovered_before)

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

            if serious_diff > 0:
                serious = "".join([serious_after, " (+", str(serious_diff), ")"])
            elif serious_diff < 0:
                serious = "".join([serious_after, " (", str(serious_diff), ")"])
            else:
                serious = serious_after

            if critical_diff > 0:
                critical = "".join([critical_after, " (+", str(critical_diff), ")"])
            elif critical_diff < 0:
                critical = "".join([critical_after, " (", str(critical_diff), ")"])
            else:
                critical = critical_after

            if recovered_diff > 0:
                recovered = "".join([recovered_after, " (+", str(recovered_diff), ")"])
            elif recovered_diff < 0:
                recovered = "".join([recovered_after, " (", str(recovered_diff), ")"])
            else:
                recovered = recovered_after

            new_data.append([location, cases, deaths, serious, recovered, critical, ""])

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
            "cases_up": "{count} new case(s) identified in **{location}**, total case(s) now are {current}",
            "cases_down": "{count} incorrectly identified case(s) in **{location}**, total case(s) now are {current}",
            "deaths_up": "{count} new death(s) recorded in **{location}**, total death(s) now are {current}",
            "deaths_down": "{count} incorrectly identified death(s) in **{location}**, total death(s) now are {current}",
            "serious_up": "{count} new serious patients identified in **{location}}**, total serious now are {current}",
            "serious_down": "{count} less serious patients in **{location}**, total serious now are {current}",
            "critical_up": "{count} new critical patients identified in **{location}}**, total critical now are {current}",
            "critical_down": "{count} less critical patients in **{location}**, total critical now are {current}",
            "recovered_up": "{count} new recovered patients identified in **{location}**, total recovered now are {current}",
            "recovered_down": "{count} incorrectly identified recovered patients in **{location}**, total recovered now are {current}",
        }

        parsed_data = self._collect_differences(data)

        message_store = []

        for _, row in parsed_data.iterrows():
            (
                location,
                cases_before,
                cases_after,
                deaths_before,
                deaths_after,
                serious_before,
                serious_after,
                critical_before,
                critical_after,
                recovered_before,
                recovered_after,
                source,
            ) = row

            cases_diff = int(cases_after) - int(cases_before)
            deaths_diff = int(deaths_after) - int(deaths_before)
            serious_diff = int(serious_after) - int(serious_before)
            critical_diff = int(critical_after) - int(critical_before)
            recovered_diff = int(recovered_after) - int(recovered_before)

            if cases_diff > 0:
                message_store.append(
                    TEXT_TEMPLATE["cases_up"].format(count=cases_diff, location=location, current=cases_after,)
                ),
            elif cases_diff < 0:
                message_store.append(
                    TEXT_TEMPLATE["cases_down"].format(count=abs(cases_diff), location=location, current=cases_after,)
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

            if serious_diff > 0:
                message_store.append(
                    TEXT_TEMPLATE["serious_up"].format(count=cases_diff, location=location, current=cases_after,)
                ),
            elif serious_diff < 0:
                message_store.append(
                    TEXT_TEMPLATE["serious_down"].format(count=cases_diff, location=location, current=cases_after,)
                ),

            if critical_diff > 0:
                message_store.append(
                    TEXT_TEMPLATE["critical_up"].format(count=cases_diff, location=location, current=cases_after,)
                ),
            elif critical_diff < 0:
                message_store.append(
                    TEXT_TEMPLATE["critical_down"].format(count=cases_diff, location=location, current=cases_after,)
                ),

            if recovered_diff > 0:
                message_store.append(
                    TEXT_TEMPLATE["recovered_up"].format(count=cases_diff, location=location, current=cases_after,)
                ),
            elif recovered_diff < 0:
                message_store.append(
                    TEXT_TEMPLATE["recovered_down"].format(count=cases_diff, location=location, current=cases_after,)
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

    def _make_embed_update(self, data, timestamp):
        parsed_data = self._collect_differences(data)

        message_store = []

        for _, row in parsed_data.iterrows():
            (
                location,
                cases_before,
                cases_after,
                deaths_before,
                deaths_after,
                serious_before,
                serious_after,
                critical_before,
                critical_after,
                recovered_before,
                recovered_after,
                source,
            ) = row

            cases_diff = int(cases_after) - int(cases_before)
            deaths_diff = int(deaths_after) - int(deaths_before)
            serious_diff = int(serious_after) - int(serious_before)
            critical_diff = int(critical_after) - int(critical_before)
            recovered_diff = int(recovered_after) - int(recovered_before)

            embed = discord.Embed(
                title=f"Coronavirus (COVID-19) update for **{location}**",
                url=source,
                timestamp=timestamp,
                colour=embed_config.EMBED_COLOUR,
            )

            embed.set_author(**embed_config.EMBED_AUTHOR)

            if embed_config.FLAG_THUMBNAIL_URL_MAPPER.get(location):
                embed.set_thumbnail(url=embed_config.FLAG_THUMBNAIL_URL_MAPPER[location])

            for field in embed_config.EMBED_FIELDS:
                if field == "cases":
                    if cases_diff > 0:
                        value = f"**{cases_after}** (+{cases_diff})"
                    elif cases_diff < 0:
                        value = f"**{cases_after}** ({cases_diff})"
                    else:
                        value = f"{cases_after}"
                elif field == "deaths":
                    if deaths_diff > 0:
                        value = f"**{deaths_after}** (+{deaths_diff})"
                    elif deaths_diff < 0:
                        value = f"**{deaths_after}** ({deaths_diff})"
                    else:
                        value = f"{deaths_after}"
                elif field == "serious":
                    if serious_diff > 0:
                        value = f"**{serious_after}** (+{serious_diff})"
                    elif serious_diff < 0:
                        value = f"**{serious_after}** ({serious_diff})"
                    else:
                        value = f"{serious_after}"
                elif field == "critical":
                    if critical_diff > 0:
                        value = f"**{critical_after}** (+{critical_diff})"
                    elif critical_diff < 0:
                        value = f"**{critical_after}** ({critical_diff})"
                    else:
                        value = f"{critical_after}"
                elif field == "recovered":
                    if recovered_diff > 0:
                        value = f"**{recovered_after}** (+{recovered_diff})"
                    elif recovered_diff < 0:
                        value = f"**{recovered_after}** ({recovered_diff})"
                    else:
                        value = f"{recovered_after}"

                embed.add_field(**embed_config.EMBED_FIELDS[field], value=value)

            message_store.append(embed)
        return message_store

    def _collect_differences(self, data):
        columns = [
            "location",
            "cases_before",
            "cases_after",
            "deaths_before",
            "deaths_after",
            "serious_before",
            "serious_after",
            "critical_before",
            "critical_after",
            "recovered_before",
            "recovered_after",
            "source",
        ]
        parsed_data = []
        changed_locations = data.Location.unique()

        for location in changed_locations:
            location_data = data.loc[data["Location"] == location]

            if len(location_data) == 1:
                cases_before = "0"
                deaths_before = "0"
                serious_before = "0"
                critical_before = "0"
                recovered_before = "0"
            else:
                cases_before = remove_non_integers_from_string(location_data.iloc[0]["Cases"])
                deaths_before = remove_non_integers_from_string(location_data.iloc[0]["Deaths"])
                serious_before = remove_non_integers_from_string(location_data.iloc[0]["Serious"])
                critical_before = remove_non_integers_from_string(location_data.iloc[0]["Critical"])
                recovered_before = remove_non_integers_from_string(location_data.iloc[0]["Recovered"])

            cases_after = remove_non_integers_from_string(location_data.iloc[-1]["Cases"])
            deaths_after = remove_non_integers_from_string(location_data.iloc[-1]["Deaths"])
            serious_after = remove_non_integers_from_string(location_data.iloc[-1]["Serious"])
            critical_after = remove_non_integers_from_string(location_data.iloc[-1]["Critical"])
            recovered_after = remove_non_integers_from_string(location_data.iloc[-1]["Recovered"])

            source = location_data.iloc[-1]["Source"]

            parsed_data.append(
                [
                    location,
                    cases_before,
                    cases_after,
                    deaths_before,
                    deaths_after,
                    serious_before,
                    serious_after,
                    critical_before,
                    critical_after,
                    recovered_before,
                    recovered_after,
                    source,
                ]
            )

        return pd.DataFrame(parsed_data, columns=columns)
