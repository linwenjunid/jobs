#!/usr/bin/python3
from celery import Celery
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

app=Celery()
app.config_from_object('app.celeryconfig')
engine = create_engine('mysql+pymysql://root:hadoop@192.168.134.201:3306/jobs?charset=utf8',pool_size=5)
DBsession = sessionmaker(bind=engine)
session = DBsession()
Base = declarative_base()

if __name__=='__main__':
    app.start()
