import pytest
import pandas as pd

from texttable import Texttable
from unittest.mock import MagicMock, patch, call

from gateways.bno_news_gateway import BnoNewsGatewayError
from services.updater_service import UpdaterService


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


@pytest.fixture(scope="function")
def stub_bno_dataframe():
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    data = [
        ["Australia", "2", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    yield pd.DataFrame(data, columns=columns)


@pytest.fixture(scope="function")
def table_updater_service():
    bno_news_gateway = MagicMock()
    data_parser_service = MagicMock()
    region = "all"
    update_interval = 1
    discord_channel_id = 1234567
    output = "table"
    logger = MagicMock()

    yield UpdaterService(
        bno_news_gateway, data_parser_service, update_interval, discord_channel_id, output, logger,
    )


@pytest.fixture(scope="function")
def text_updater_service():
    bno_news_gateway = MagicMock()
    data_parser_service = MagicMock()
    region = "all"
    update_interval = 1
    discord_channel_id = 1234567
    output = "text"
    logger = MagicMock()

    yield UpdaterService(
        bno_news_gateway, data_parser_service, update_interval, discord_channel_id, output, logger,
    )


def test_collecting_differences(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "3", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "3", "0", "0", "0", ""],
        ["Austria", "1", "0", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)
    result = table_updater_service._collect_differences(joined_data)

    expected_result = pd.DataFrame(
        [
            ["Australia", "2", "3", "1", "1", "1 serious"],
            ["Sweden", "5", "5", "2", "3", ""],
            ["Austria", "0", "1", "0", "0", ""],
        ],
        columns=["location", "cases_before", "cases_after", "deaths_before", "deaths_after", "notes",],
    )

    pd.testing.assert_frame_equal(expected_result, result)


def test_collecting_differences_with_thousands_seperated_by_commas(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "3,000", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "3", "0", "0", "0", ""],
        ["Austria", "1", "0", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)
    result = table_updater_service._collect_differences(joined_data)

    expected_result = pd.DataFrame(
        [
            ["Australia", "2", "3000", "1", "1", "1 serious"],
            ["Sweden", "5", "5", "2", "3", ""],
            ["Austria", "0", "1", "0", "0", ""],
        ],
        columns=["location", "cases_before", "cases_after", "deaths_before", "deaths_after", "notes",],
    )

    pd.testing.assert_frame_equal(expected_result, result)


def test_text_update_cases_up_single(text_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "3", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = text_updater_service._make_update_message(joined_data)

    expected_messages = ["1 new case(s) identified in **Australia**, total case(s) now are 3"]

    assert expected_messages == messages


def test_text_update_cases_up_multi(text_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "3", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "6", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = text_updater_service._make_update_message(joined_data)

    expected_messages = [
        "1 new case(s) identified in **Australia**, total case(s) now are 3\n1 new case(s) identified in **Sweden**, total case(s) now are 6"
    ]

    assert expected_messages == messages


def test_text_update_cases_down_single(text_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "1", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = text_updater_service._make_update_message(joined_data)

    expected_messages = ["1 incorrectly identified case(s) in **Australia**, total case(s) now are 1"]

    assert expected_messages == messages


def test_text_update_cases_down_multi(text_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "1", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "4", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = text_updater_service._make_update_message(joined_data)

    expected_messages = [
        "1 incorrectly identified case(s) in **Australia**, total case(s) now are 1\n1 incorrectly identified case(s) in **Sweden**, total case(s) now are 4"
    ]

    assert expected_messages == messages


def test_text_update_deaths_up_single(text_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "2", "2", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = text_updater_service._make_update_message(joined_data)

    expected_messages = ["1 new death(s) recorded in **Australia**, total death(s) now are 2"]

    assert expected_messages == messages


def test_text_update_deaths_up_multi(text_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "2", "2", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "3", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = text_updater_service._make_update_message(joined_data)

    expected_messages = [
        "1 new death(s) recorded in **Australia**, total death(s) now are 2\n1 new death(s) recorded in **Sweden**, total death(s) now are 3"
    ]

    assert expected_messages == messages


def test_text_update_deaths_down_single(text_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "2", "0", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = text_updater_service._make_update_message(joined_data)

    expected_messages = ["1 incorrectly identified death(s) in **Australia**, total death(s) now are 0"]

    assert expected_messages == messages


def test_text_update_deaths_down_multi(text_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "2", "0", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "1", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = text_updater_service._make_update_message(joined_data)

    expected_messages = [
        "1 incorrectly identified death(s) in **Australia**, total death(s) now are 0\n1 incorrectly identified death(s) in **Sweden**, total death(s) now are 1"
    ]

    assert expected_messages == messages


def test_table_update_cases_up_single(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "3", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = table_updater_service._make_update_message(joined_data)

    table = Texttable()

    table.set_cols_align(["c", "c", "c", "c", "c", "c", "c"])
    table.set_cols_valign(["m", "m", "m", "m", "m", "m", "m"])

    expected_rows = [
        ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"],
        ["Australia", "3 (+1)", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    table.add_rows(expected_rows)
    expected_table = [f"```{table.draw()}```"]

    assert expected_table == messages


def test_table_update_cases_up_multiple(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "3", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "6", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = table_updater_service._make_update_message(joined_data)

    table = Texttable()

    table.set_cols_align(["c", "c", "c", "c", "c", "c", "c"])
    table.set_cols_valign(["m", "m", "m", "m", "m", "m", "m"])

    expected_rows = [
        ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"],
        ["Australia", "3 (+1)", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "6 (+1)", "2", "0", "0", "0", ""],
    ]

    table.add_rows(expected_rows)
    expected_table = [f"```{table.draw()}```"]

    assert expected_table == messages


def test_table_update_cases_down_single(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "1", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = table_updater_service._make_update_message(joined_data)

    table = Texttable()

    table.set_cols_align(["c", "c", "c", "c", "c", "c", "c"])
    table.set_cols_valign(["m", "m", "m", "m", "m", "m", "m"])

    expected_rows = [
        ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"],
        ["Australia", "1 (-1)", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    table.add_rows(expected_rows)
    expected_table = [f"```{table.draw()}```"]

    assert expected_table == messages


def test_table_update_cases_down_multi(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "1", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "4", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = table_updater_service._make_update_message(joined_data)

    table = Texttable()

    table.set_cols_align(["c", "c", "c", "c", "c", "c", "c"])
    table.set_cols_valign(["m", "m", "m", "m", "m", "m", "m"])

    expected_rows = [
        ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"],
        ["Australia", "1 (-1)", "1", "0", "0", "0", "1 serious"],
        ["Sweden", "4 (-1)", "2", "0", "0", "0", ""],
    ]

    table.add_rows(expected_rows)
    expected_table = [f"```{table.draw()}```"]

    assert expected_table == messages


def test_table_update_deaths_up_single(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "2", "2", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = table_updater_service._make_update_message(joined_data)

    table = Texttable()

    table.set_cols_align(["c", "c", "c", "c", "c", "c", "c"])
    table.set_cols_valign(["m", "m", "m", "m", "m", "m", "m"])

    expected_rows = [
        ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"],
        ["Australia", "2", "2 (+1)", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    table.add_rows(expected_rows)
    expected_table = [f"```{table.draw()}```"]

    assert expected_table == messages


def test_table_update_deaths_up_multi(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "2", "2", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "3", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = table_updater_service._make_update_message(joined_data)

    table = Texttable()

    table.set_cols_align(["c", "c", "c", "c", "c", "c", "c"])
    table.set_cols_valign(["m", "m", "m", "m", "m", "m", "m"])

    expected_rows = [
        ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"],
        ["Australia", "2", "2 (+1)", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "3 (+1)", "0", "0", "0", ""],
    ]

    table.add_rows(expected_rows)
    expected_table = [f"```{table.draw()}```"]

    assert expected_table == messages


def test_table_update_deaths_down_single(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "2", "0", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = table_updater_service._make_update_message(joined_data)

    table = Texttable()

    table.set_cols_align(["c", "c", "c", "c", "c", "c", "c"])
    table.set_cols_valign(["m", "m", "m", "m", "m", "m", "m"])

    expected_rows = [
        ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"],
        ["Australia", "2", "0 (-1)", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "2", "0", "0", "0", ""],
    ]

    table.add_rows(expected_rows)
    expected_table = [f"```{table.draw()}```"]

    assert expected_table == messages


def test_table_update_deaths_down_multi(table_updater_service, stub_bno_dataframe):
    columns = [
        "Location",
        "Cases",
        "Deaths",
        "Serious",
        "Critical",
        "Recovered",
        "Notes",
    ]
    new_data = [
        ["Australia", "2", "0", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "1", "0", "0", "0", ""],
    ]

    data_after = pd.DataFrame(new_data, columns=columns)
    joined_data = stub_bno_dataframe.append(data_after)

    messages = table_updater_service._make_update_message(joined_data)

    table = Texttable()

    table.set_cols_align(["c", "c", "c", "c", "c", "c", "c"])
    table.set_cols_valign(["m", "m", "m", "m", "m", "m", "m"])

    expected_rows = [
        ["Location", "Cases", "Deaths", "Serious", "Critical", "Recovered", "Notes"],
        ["Australia", "2", "0 (-1)", "0", "0", "0", "1 serious"],
        ["Sweden", "5", "1 (-1)", "0", "0", "0", ""],
    ]

    table.add_rows(expected_rows)
    expected_table = [f"```{table.draw()}```"]

    assert expected_table == messages


@pytest.mark.asyncio
async def test_fetching_latest_data_failure_handled(table_updater_service):
    discord_client = MagicMock()
    discord_client.is_closed.side_effect = [False, True]

    table_updater_service.bno_news_gateway.fetch_raw.side_effect = [BnoNewsGatewayError("Test")]

    with patch("services.updater_service.asyncio.sleep", new_callable=AsyncMock):
        await table_updater_service.update_loop(discord_client)

    table_updater_service.logger.critical.assert_called_once_with("Failed to fetch the latest virus data - Test")
