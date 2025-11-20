import multiprocessing

bind = "0.0.0.0:8080"
workers = max(1, multiprocessing.cpu_count() // 2)
worker_class = "sync"
timeout = 90
keepalive = 5
loglevel = "info"
