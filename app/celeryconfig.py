from datetime import timedelta
from celery.schedules import crontab
from kombu import Exchange, Queue

#时区
CELERY_TIMEZONE = 'Asia/Shanghai'
#消息中间件
BROKER_URL = 'redis://:hadoop@192.168.134.151:6379/0'
BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 3600}  # 1 hour.
BROKER_TRANSPORT_OPTIONS = {'fanout_prefix': True}
#结果存储
CELERY_RESULT_BACKEND = 'redis://:hadoop@192.168.134.151:6379/1'
#注册任务
CELERY_IMPORTS = ("app.tasks",)

#定义交换机
default_exchange = Exchange('dedfault', type='direct')
job_exchange    = Exchange('job', type='direct')

#定义队列
CELERY_QUEUES = (
    Queue('default', default_exchange, routing_key='default'),
    Queue('working', job_exchange, routing_key='working'),
    Queue('firing', job_exchange, routing_key='firing'),
    Queue('queueing', job_exchange, routing_key='queueing')
)

#设置默认的交换机、队列、绑定规则
CELERY_DEFAULT_QUEUE = 'default'
CELERY_DEFAULT_EXCHANGE = 'default'
CELERY_DEFAULT_ROUTING_KEY = 'default'

CELERY_ROUTES = ({'app.tasks.working': {
                        'queue': 'working',
                        'routing_key': 'working'
                 }},{'app.tasks.firing': {
                        'queue': 'firing',
                        'routing_key': 'firing'
                 }},{'app.tasks.queueing':{
                        'queue': 'queueing',
                        'routing_key': 'queueing'
                 }}
                 )

"""
在脚本同级目录执行celery -A tasks worker -B，即启动worker和beat服务；
或者先用celery -A proj worker –loglevel=INFO启动worker，再
用celery -A tasks beat -s celerybeat-schedule 
#这里的celerybeat-schedule指定一个记录文件**启动beat服务也行。 
"""
CELERYBEAT_SCHEDULE = {
    'working': {
        'task': 'app.tasks.working',
        'schedule': timedelta(seconds=5),
        'args': (),
    },
    'queueing': {
        'task': 'app.tasks.queueing',
        'schedule': timedelta(seconds=5),
        'args': (),
    },
    "firing": {
        "task": "app.tasks.firing",
        "schedule": crontab(hour="*", minute="*/2"),
        "args": (),
    }
}
