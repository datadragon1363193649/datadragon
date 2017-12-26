#! /usr/bin/env python
# -*- encoding: utf-8 -*-

from sqlalchemy import Column, INT,VARCHAR,SMALLINT,Boolean, BIGINT,DateTime,FLOAT,Date
#from orm import Base
from bin.api.controller.common.orm import Base

class Source_type(Base):
    __tablename__ = 'uc_users'
    id = Column(BIGINT, primary_key=True)
    sourceType = Column(INT)
    channelSource = Column(VARCHAR(255))
    lastLoginClient = Column(INT)
    createDate = Column(DateTime)
class Contact(Base):
    __tablename__ = 'risk_auth_contact_person'

    id = Column(BIGINT,primary_key = True)
    contactMobile1 = Column(VARCHAR(255))
    contactMobile2 = Column(VARCHAR(255))
    userId = Column(BIGINT)
class CardId(Base):
    __tablename__ = 'risk_auth_cardid'

    id = Column(BIGINT, primary_key=True)
    sex = Column(VARCHAR(5))
    age = Column(INT)
    userId = Column(BIGINT)
    validEndDate=Column(Date)

class RiskUser(Base):
    __tablename__ = 'risk_user'

    id = Column(BIGINT,primary_key = True)
    mobile=Column(VARCHAR(64))
class CreditTask(Base):
    __tablename__ = 'risk_auth_credit_task'

    id = Column(BIGINT, primary_key=True)
    userId = Column(BIGINT)
    userName = Column(VARCHAR(255))
class UnioznpayTask(Base):
    __tablename__ = 'risk_wecash_unionpay_detail'

    id = Column(BIGINT, primary_key=True)
    userId = Column(BIGINT)
    mobile=Column(VARCHAR(64))
    resultDetail=Column(VARCHAR(5000))
class AuthFace(Base):
    __tablename__ = 'risk_auth_face'

    id = Column(BIGINT, primary_key=True)
    user_id = Column(BIGINT)
    create_date=Column(DateTime)
    update_date = Column(DateTime)
    face_verification_confidence = Column(FLOAT)
    face_verification_num=Column(INT)
    identity_face_verification_confidence=Column(FLOAT)
class PersonInfo(Base):
    __tablename__ = 'risk_user_person_info'

    id = Column(BIGINT, primary_key=True)
    userId = Column(BIGINT)
    degree=Column(VARCHAR(255))
    maritalStatus = Column(VARCHAR(255))
class WorkInfo(Base):
    __tablename__ = 'risk_user_work_info'

    id = Column(BIGINT, primary_key=True)
    userId = Column(BIGINT)
    industry=Column(VARCHAR(255))
    jobTitle = Column(VARCHAR(255))
    monthlyIncome = Column(VARCHAR(255))
class UserCard(Base):
    __tablename__ = 'risk_user_card'

    id = Column(BIGINT, primary_key=True)
    userId = Column(BIGINT)
    createDate = Column(DateTime)
    product=Column(VARCHAR(255))
class CreditChain(Base):
    __tablename__ = 'risk_credit_chain'

    id = Column(BIGINT, primary_key=True)
    userId = Column(BIGINT)
    createDate = Column(DateTime)
    audit_date = Column(DateTime)
    pattern=Column(VARCHAR(255))
class UserMerchant(Base):
    __tablename__ = 'risk_user_merchant'

    id = Column(BIGINT, primary_key=True)
    userId = Column(BIGINT)
    merchantUserId = Column(VARCHAR(255))
class BehaviorTask(Base):
    __tablename__ = 'risk_wecash_credit_behavior'

    id = Column(BIGINT, primary_key=True)
    userId = Column(BIGINT)
    mobile = Column(VARCHAR(255))
    applicationPlatforms=Column(INT)
    applicationCount=Column(INT)
    registrationPlatforms=Column(INT)
    registrationCount=Column(INT)
    overduePlatforms=Column(INT)
    overdueCount=Column(INT)
    loanlendersSixMonthsCount=Column(INT)
    loanlendersTwelveMonthsCount=Column(INT)
    loanlendersCount=Column(INT)
    loanlendersSixMonthsPlatforms=Column(INT)
    loanlendersTwelveMonthsPlatforms=Column(INT)
    loanlendersPlatforms=Column(INT)
    rejectionSixMonthsCount=Column(INT)
    rejectionTwelveMonthsCount=Column(INT)
    rejectionCount=Column(INT)
    rejectionSixMonthsPlatforms=Column(INT)
    rejectionTwelveMonthsPlatforms=Column(INT)
    rejectionPlatforms=Column(INT)
