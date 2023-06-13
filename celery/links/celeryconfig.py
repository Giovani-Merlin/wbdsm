from datetime import timedelta

broker_url = "redis://localhost:6379/0"
# backend uses a lot of memory
# backend_url = "redis://localhost:6379/1"
result_expires = timedelta(minutes=5)
imports = ("extract_links_task",)
task_ignore_result = True
worker_prefetch_multiplier = 10  # default is 4
