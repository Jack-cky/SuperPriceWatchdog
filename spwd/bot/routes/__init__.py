from .alert import bp as bp_alert
from .error import bp as bp_errors
from .health import bp as bp_health
from .index import bp as bp_index
from .response import bp as bp_response
from .robots import bp as bp_robots
from .webhook import bp as bp_webhook

__all__ = [
    "bp_alert",
    "bp_errors",
    "bp_health",
    "bp_index",
    "bp_response",
    "bp_robots",
    "bp_webhook",
]
