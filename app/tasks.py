from . import app,session
from .models import Job,Job_Log,Job_Queue
from sqlalchemy import func
from datetime import datetime, date, timedelta

#触发起点
@app.task
def firing():
    try:
        print('----Fire Start----') 
        job=session.query(Job).filter(Job.id==1).first()
        job.start_time=datetime.now() 
        job.end_time=datetime.now()
        if job.job_date:
            job.job_date=job.job_date+timedelta(days = 1)
        else:
            job.job_date=date.today()
        session.add(job)

        log=Job_Log(job)
        session.add(log)
        
        job.queueing()

        print('---- Fire End ----')
        return True
    except Exception as e:
        session.rollback()
        print(e)
        return False
    finally:
        session.commit()

#Queue转Pending
@app.task
def triggering():
    try:
        print('----Queue Start----')
        queues=session.query(Job_Queue.job_id,func.min(Job_Queue.job_date).label('job_date')).\
                       filter(Job.id==Job_Queue.job_id,
                              Job.job_status=='Done',
                              Job.job_enable==1).group_by(Job_Queue.job_id).all()
        for queue in queues:
            job=session.query(Job).filter(Job.id==queue.job_id).first()
            if job.pending(queue.job_date):
                Job_Queue.del_queue(queue.job_id,queue.job_date)
        print('----Queue End----')
        return True
    except Exception as e:
        session.rollback()
        print(e)
        return False
    finally:
        session.commit()

#启动任务Pending转Running
@app.task
def working():
    try:
        print('----Work Start----')
        jobs=session.query(Job).filter(Job.job_status=='Pending',Job.job_enable==1).all()
        for job in jobs:
            if job.running():
                runjob.delay(job.id)
        print('---- Work End ----')
        return True
    except Exception as e:
        session.rollback()
        print(e)
        return False
    finally:
        session.commit()

#运行任务Running转Done触发Queue
@app.task
def runjob(id):
    try:
        job=session.query(Job).filter(Job.id==id).first()
        print('----Start----')
        print('任务编码:[%d]'%job.id)
        print('任务名称:[%s]'%job.job_name)

        import time,random
        t=random.randint(5,10)
        time.sleep(t)
        print('任务耗时:%d秒'%t)
        if t%2==0:
            if job.done():
                job.queueing()
        else:
            if job.failed():
                print('任务运行失败')

        log=Job_Log(job)
        session.add(log)
        session.commit()
        print('----END----')
        return True
    except Exception as e:
        session.rollback()
        print(e)
        return False
    finally:
        session.commit()

