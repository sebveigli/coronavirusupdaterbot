# Coronavirus Updater Discord Bot
A bot that alerts you on newly discovered cases of Novel Coronavirus (2019-nCoV) around the world as soon as data is fetched.

## About the bot
As the spread of Novel Coronavirus has started to impact a global scale, it can be hard to keep up to date with the flood of daily reports coming in from different countries regarding the spread of the virus in their regions.

I wanted a way to be able to keep on top of the spread, and a Discord bot was the most convienient.

The Coronavirus Updater Bot scrapes data from BNO News (https://bnonews.com/index.php/2020/02/the-latest-coronavirus-cases/) at a defined interval, and determines if any updates have been made since the last check.

In case of any new updates, the bot will notify the defined channel, either in text or table format.

## Bot implementation & usage
The bot is written in Python 3.7, with Python 3.6 being the minimum supported version (sorry - f-strings are just *too* nice).

All dependencies can be installed using pip -r requirements.txt into your local environment.

```
Usage: python app.py [flags]

  -h/--help: Show help
  -t/--token: Your bot's Discord token (found at https://discordapp.com/developers/applications/)
  -c/--channel: The channel ID that the bot should report updates to
  -f/--frequency: How often the bot should scrape BNO for new updates
  -r/--region {all, china, international}: Which data should be parsed from BNO
  -o/--output {text, table}: Whether the output to Discord should be in text (sentences) or table format
  -s/--severity {debug, info, warning, error, critical}: Logging severity to the standard output (default: info)
  ```

### Contributing
This bot will be hosted on a server shortly, which will allow people to invite the bot into their Discord servers without having to run it on their machine. If there are any feature requests, bugs or issues, please report them through GitHub, or feel free to submit a Pull Request.
