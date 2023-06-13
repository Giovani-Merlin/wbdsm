import os
from datetime import timedelta

from celery import Celery, Task, bootsteps
from celery.bin import Option

app = Celery(
    "Extract mentions project",
)

default_config = "celeryconfig"
app.config_from_object(default_config)
# Add options to worker
app.user_options["worker"].add(
    Option(
        "--mongo_uri",
        dest="mongo_uri",
        default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB URI",
    )
)
app.user_options["worker"].add(
    Option(
        "--language",
        dest="language",
        default=os.environ.get("LANGUAGE", "en"),
        help="Language to use",
    )
)
