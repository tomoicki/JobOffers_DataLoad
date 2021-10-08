from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import shortuuid

Base = declarative_base()

#  association Tables
association_JobOffer_Location = Table('association_JobOffer_Location', Base.metadata,
                                      Column('job_offer_id', ForeignKey('JobOffer.id'), primary_key=True),
                                      Column('location_id', ForeignKey('Location.id'), primary_key=True)
                                      )
association_JobOffer_Experience = Table('association_JobOffer_Experience', Base.metadata,
                                        Column('job_offer_id', ForeignKey('JobOffer.id'),
                                               primary_key=True),
                                        Column('experience_id', ForeignKey('Experience.id'),
                                               primary_key=True)
                                        )
association_JobOffer_EmploymentType = Table('association_JobOffer_EmploymentType', Base.metadata,
                                            Column('job_offer_id', ForeignKey('JobOffer.id'),
                                                   primary_key=True),
                                            Column('employment_type_id',
                                                   ForeignKey('EmploymentType.id'),
                                                   primary_key=True)
                                            )
association_JobOffer_Skill_must = Table('association_JobOffer_Skill_must', Base.metadata,
                                        Column('job_offer_id', ForeignKey('JobOffer.id'),
                                               primary_key=True),
                                        Column('skill_id', ForeignKey('Skill.id'), primary_key=True)
                                        )
association_JobOffer_Skill_nice = Table('association_JobOffer_Skill_nice', Base.metadata,
                                        Column('job_offer_id', ForeignKey('JobOffer.id'),
                                               primary_key=True),
                                        Column('skill_id', ForeignKey('Skill.id'), primary_key=True)
                                        )


#  main Tables
class JobOffer(Base):
    __tablename__ = 'JobOffer'
    #  fields
    id = Column(String, primary_key=True)
    title = Column(String)
    b2b_min = Column(Integer)
    b2b_max = Column(Integer)
    permanent_min = Column(Integer)
    permanent_max = Column(Integer)
    mandate_min = Column(Integer)
    mandate_max = Column(Integer)
    expired = Column(String)
    expired_at = Column(String)
    scraped_at = Column(String)
    offer_url = Column(String)

    def __init__(self, title, b2b_min, b2b_max, permanent_min, permanent_max,
                 mandate_min, mandate_max, expired, scraped_at, offer_url):
        self.id = shortuuid.uuid()
        self.title = title,
        self.b2b_min = b2b_min,
        self.b2b_max = b2b_max,
        self.permanent_min = permanent_min,
        self.permanent_max = permanent_max,
        self.mandate_min = mandate_min,
        self.mandate_max = mandate_max,
        self.expired = expired,
        self.scraped_at = scraped_at,
        self.offer_url = offer_url

    #  many to one
    company_id = Column(String, ForeignKey('Company.id'))
    to_company = relationship("Company",
                              back_populates="to_job_offer")
    jobsite_id = Column(String, ForeignKey('Jobsite.id'))
    to_jobsite = relationship("Jobsite",
                              back_populates="to_job_offer")
    #  many to many
    to_location = relationship("Location",
                               secondary=association_JobOffer_Location,
                               back_populates='to_job_offer')
    to_experience = relationship("Experience",
                                 secondary=association_JobOffer_Experience,
                                 back_populates='to_job_offer')
    to_employment_type = relationship("EmploymentType",
                                      secondary=association_JobOffer_EmploymentType,
                                      back_populates='to_job_offer')
    to_skill_must = relationship("Skill",
                                 secondary=association_JobOffer_Skill_must,
                                 back_populates='skill_must_to_job_offer')
    to_skill_nice = relationship("Skill",
                                 secondary=association_JobOffer_Skill_nice,
                                 back_populates='skill_nice_to_job_offer')


class Company(Base):
    __tablename__ = 'Company'
    #  fields
    id = Column(String, primary_key=True)
    name = Column(String)
    size = Column(String)

    def __init__(self, name, size):
        self.id = shortuuid.uuid()
        self.name = name
        self.size = size

    #  one to many
    to_job_offer = relationship("JobOffer",
                                back_populates='to_company')


class Jobsite(Base):
    __tablename__ = 'Jobsite'
    url_list = ['https://nofluffjobs.com/', 'https://justjoin.it/']
    #  fields
    id = Column(String, primary_key=True)
    name = Column(String)
    url = Column(String)

    def __init__(self, name):
        self.id = shortuuid.uuid()
        self.name = name
        for item in self.url_list:
            if name in item:
                self.url = item

    #  one to many
    to_job_offer = relationship("JobOffer",
                                back_populates='to_jobsite')


class Location(Base):
    __tablename__ = 'Location'
    #  fields
    id = Column(String, primary_key=True)
    name = Column(String)

    def __init__(self, name):
        self.id = shortuuid.uuid()
        self.name = name

    #  many to many
    to_job_offer = relationship("JobOffer",
                                secondary=association_JobOffer_Location,
                                back_populates='to_location')


class Experience(Base):
    __tablename__ = 'Experience'
    #  fields
    id = Column(String, primary_key=True)
    level = Column(String)

    def __init__(self, level):
        self.id = shortuuid.uuid()
        self.level = level

    #  many to many
    to_job_offer = relationship("JobOffer",
                                secondary=association_JobOffer_Experience,
                                back_populates='to_experience')


class EmploymentType(Base):
    __tablename__ = "EmploymentType"
    #  fields
    id = Column(String, primary_key=True)
    type = Column(String)

    def __init__(self, emp_type):
        self.id = shortuuid.uuid()
        self.type = emp_type

    #  many to many
    to_job_offer = relationship("JobOffer",
                                secondary=association_JobOffer_EmploymentType,
                                back_populates='to_employment_type')


class Skill(Base):
    __tablename__ = "Skill"
    #  fields
    id = Column(String, primary_key=True)
    name = Column(String)

    def __init__(self, name):
        self.id = shortuuid.uuid()
        self.name = name

    # many to many
    skill_must_to_job_offer = relationship("JobOffer",
                                           secondary=association_JobOffer_Skill_must,
                                           back_populates='to_skill_must')
    skill_nice_to_job_offer = relationship("JobOffer",
                                           secondary=association_JobOffer_Skill_nice,
                                           back_populates='to_skill_nice')
