from enum import IntEnum


class PromoCategory(IntEnum):
    NA = 0                # default
    AMT_AMT = 1           # two amounts (NA - not commonly parseable)
    AMT_FOR_CNT = 2       # $X for N items
    AMT_PCT = 3           # amount and percent (NA)
    BUY_N_SAVE_AMT = 4    # buy N at/save $X
    BUY_N_GET_N_FREE = 5  # buy N get N free
    BUY_N_PCT_OFF = 6     # buy N at X% off
    PCT_AMT = 7           # percent and amount (NA)
    PCT_FOR_N = 8         # X% for N items
    PCT_PCT = 9           # two percents (NA)
