利用芹菜并发调度复杂依赖作业（蛛网状态依赖）

/celery/redis-2.8.17/src/redis-server /celery/redis-2.8.17/redis.conf

celery -n n1 -A app worker -l info -Q default
celery -n n5 -A app worker -l info -Q default
celery -n n2 -A app worker -l info -Q firing
celery -n n3 -A app worker -l info -Q working
celery -n n4 -A app worker -l info -Q triggering
celery -A app beat

import datetime
from app.tasks import firing
d=datetime.date(2018,8,10)
firing.delay(d)
