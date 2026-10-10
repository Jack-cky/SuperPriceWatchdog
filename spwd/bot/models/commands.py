from enum import StrEnum


class ExternalSlash(StrEnum):
    START = "/start"    # greet the user and register them
    HELP = "/help"      # provide a help message with instructions for the user
    LIST = "/list"      # retrieve and list all items the user is currently tracking
    SUB = "/sub"        # update the user's subscription status for daily alerts
    LUCKY = "/lucky"    # get a list of randomly selected best deals for the day
    LANG = "/lang"      # change the user's preferred language for responses
    SNOOZE = "/snooze"  # pause daily alerts until the next promotion week


class InternalSlash(StrEnum):
    ALERT = "/alert"  # get a list of best deals for the day
    PLOT = "/plot"    # generate a time series plot for a specific item's price trends
    EDIT = "/edit"    # update the user's watchlist by adding or removing items
    ERROR = "/error"  # generate an error message based on the provided status


class HiddenSlash(StrEnum):
    BYE = "/bye"      # unregister the user and provide a farewell message
