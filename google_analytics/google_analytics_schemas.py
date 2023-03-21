from __future__ import annotations

from typing import Final, List, TypedDict

_SESSION_DIMENSIONS: Final[List[TypedDict[str, str]]] = [
    {
        "name": "ga:clientId"
    },
    {
        "name": "ga:dateHourMinute"
    },
    # {
    #     "name": "ga:date"
    # },
    {
        "name": "ga:sourceMedium"
    },
    {
        "name": "ga:adwordsCampaignID"
    },
    # {
    #     "name": "ga:campaign"
    # }
]

_TRANSACTION_DIMENSIONS: Final[List[TypedDict[str, str]]] = [
    {
        "name": "ga:clientId"
    },
    {
        "name": "ga:dateHourMinute"
    },
    {
        "name": "ga:sourceMedium"
    },
    {
        "name": "ga:adwordsCampaignID"
    },
    # {
    #     "name": "ga:campaign"
    # },
    {
        "name": "ga:transactionId"
    }
]

_SESSION_METRICS: Final[List[TypedDict[str, str]]] = [
    # {
    #     "expression": "ga:goal14Completions"
    # },
    # {
    #     "expression": "ga:goal3Completions"
    # },
    # {
    #     "expression": "ga:newUsers"
    # },
    {
        "expression": "ga:sessions"
    }
]

_TRANSACTION_METRICS: Final[List[TypedDict[str, str]]] = [
    {
        "expression": "ga:transactions"
    }
    # ,
    # {
    #     "expression": "ga:transactionRevenue"
    # }
]
