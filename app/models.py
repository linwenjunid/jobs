from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Date
from sqlalchemy.orm import relationship,backref
from datetime import datetime
from . import session,Base

class Job_Log(Base):
    __tablename__='job_logs'
    id         = Column(Integer,primary_key=True)
    job_id     = Column(Integer,ForeignKey('jobs.id'))
    start_time = Column(DateTime())
    end_time   = Column(DateTime())
    job_date   = Column(Date())
    job_status = Column(String(64))

    def __init__(self,job):
        self.job_id     = job.id
        self.start_time = job.start_time
        self.end_time   = job.end_time
        self.job_date   = job.job_date
        self.job_status = job.job_status

class Job_Queue(Base):
    __tablename__='job_queues'
    job_id     = Column(Integer,ForeignKey('jobs.id'), primary_key=True)
    job_date   = Column(Date(), primary_key=True)

    def __init__(self,id,date):
        self.job_id=id
        self.job_date=date
    
    @staticmethod
    def del_queue(id,date):
        try:
            session.query(Job_Queue).filter(Job_Queue.job_id==id,Job_Queue.job_date==date).delete()
            session.flush()
            print('任务[%d][%s]成功Pending,删除队列'%(id,date))
        except Exception as e:
            print(e)
            session.rollback()
            return False
        else:
            session.commit()
            return True
    
    @staticmethod
    def add_queue(id,date):
        try:
            queue=Job_Queue(id,date)
            session.add(queue)
            session.flush()
            print('任务[%d][%s]加入队列'%(queue.job_id,queue.job_date))
        except Exception as e:
            print(e)
            session.rollback()
            return False
        else:
            session.commit()
            return True

class Trigger(Base):
    __tablename__='triggers'
    trigger_source_id = Column(Integer,ForeignKey('jobs.id'),primary_key=True)
    trigger_target_id = Column(Integer,ForeignKey('jobs.id'),primary_key=True)
    job_date   = Column(Date())

class Job(Base):
    __tablename__='jobs'
    id         = Column(Integer,primary_key=True)
    job_name   = Column(String(64),unique=True,index=True)
    job_desc   = Column(String(64))
    job_status = Column(String(64))
    job_enable = Column(Boolean,default=False)
    start_time = Column(DateTime())
    end_time   = Column(DateTime())
    job_date   = Column(Date())
    job_type   = Column(String(64),default='default')
    job_worker = Column(String(64))
    job_logs   = relationship('Job_Log',backref=backref('job'),lazy='dynamic')
    job_queues = relationship('Job_Queue',backref=backref('job'),lazy='dynamic')
    trigger_target = relationship('Trigger',
                                foreign_keys=[Trigger.trigger_source_id],
                                backref=backref('trigger_source',lazy='joined'),
                                lazy='dynamic',
                                cascade='all,delete-orphan')
    trigger_source = relationship('Trigger',
                                foreign_keys=[Trigger.trigger_target_id],
                                backref=backref('trigger_target',lazy='joined'),
                                lazy='dynamic',
                                cascade='all,delete-orphan')

    def done(self):
        try:
            self.job_status='Done'
            self.end_time=datetime.now()
            session.add(self)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
            return False
        else:
            return True

    def pending(self,d):
        try:
           self.job_status='Pending'
           self.job_date=d
           self.start_time=datetime.now()
           self.end_time= None
           session.add(self)
           session.commit()
           return True
        except Exception as e:
            print(e)
            session.rollback()
            return False

    def running(self):
        try:
            t_count=session.query(Trigger,Job).filter(Trigger.trigger_source_id==self.id, 
                                                      Trigger.trigger_target_id==Job.id,
                                                      Job.job_status=='Running', 
                                                      Job.job_enable==1).count()
            s1_count=session.query(Trigger,Job).filter(Trigger.trigger_target_id==self.id, 
                                                      Trigger.trigger_source_id==Job.id,
                                                      Job.job_date<self.job_date, 
                                                      Job.job_enable==1).count()
            s2_count=session.query(Trigger,Job).filter(Trigger.trigger_target_id==self.id,
                                                      Trigger.trigger_source_id==Job.id,
                                                      Job.job_date==self.job_date,
                                                      Job.job_status!='Done',
                                                      Job.job_enable==1).count()
            s3_count=session.query(Trigger,Job).filter(Trigger.trigger_target_id==self.id,
                                                      Trigger.trigger_source_id==Job.id,
                                                      Job.job_date>self.job_date,
                                                      Job.job_status=='Running',
                                                      Job.job_enable==1).count()
            if t_count|s1_count|s2_count|s3_count==0:
                self.job_status='Running'
                self.start_time=datetime.now()
                session.add(self)
                session.commit()
                print('任务[%d][%s]满足条件,Running'%(self.id,self.job_date))
                return True
            else:
                print('任务[%d][%s]不满足条件,Waiting'%(self.id,self.job_date))
                return False
        except Exception as e:
            print(e)
            session.rollback()
            return False

    def trigger_queueing(self):
        try:
            jobs=session.query(Job).filter(Trigger.trigger_source_id==self.id,
                                           Trigger.trigger_target_id==Job.id,
                                           Job.job_enable==1).all()
            for job in jobs:
                queue=session.query(Job_Queue).filter(Job_Queue.job_id==job.id,Job_Queue.job_date==self.job_date).first()
                if not queue and not (job.job_status=='Pending' and job.job_date==self.job_date):
                    Job_Queue.add_queue(job.id,self.job_date)
        except Exception as e:
            session.rollback()
            print(e)
            return False
        else:
            session.commit()
            return True

#    def initdb():
#        Base.metadata.drop_all(engine)
#        Base.metadata.create_all(engine)

