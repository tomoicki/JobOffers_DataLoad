from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import shortuuid


Base = declarative_base()

#  association Tables
association_JobOffer_Location = Table('association_JobOffer_Location', Base.metadata,
                                      Column('job_offer_id', ForeignKey('JobOffer.offer_url'), primary_key=True),
                                      Column('location', ForeignKey('Location.location'), primary_key=True)
                                      )
association_JobOffer_Experience = Table('association_JobOffer_Experience', Base.metadata,
                                        Column('job_offer_id', ForeignKey('JobOffer.offer_url'),
                                               primary_key=True),
                                        Column('experience', ForeignKey('Experience.experience'),
                                               primary_key=True)
                                        )
association_JobOffer_Employment_type = Table('association_JobOffer_Employment_type', Base.metadata,
                                             Column('job_offer_id', ForeignKey('JobOffer.offer_url'),
                                                    primary_key=True),
                                             Column('employment_type',
                                                    ForeignKey('Employment_type.employment_type'),
                                                    primary_key=True)
                                             )
association_JobOffer_Skill_must = Table('association_JobOffer_Skill_must', Base.metadata,
                                        Column('job_offer_id', ForeignKey('JobOffer.offer_url'),
                                               primary_key=True),
                                        Column('skill', ForeignKey('Skill.skill'), primary_key=True)
                                        )
association_JobOffer_Skill_nice = Table('association_JobOffer_Skill_nice', Base.metadata,
                                        Column('job_offer_id', ForeignKey('JobOffer.offer_url'),
                                               primary_key=True),
                                        Column('skill', ForeignKey('Skill.skill'), primary_key=True)
                                        )


#  main Tables
class JobOffer(Base):
    __tablename__ = 'JobOffer'
    #  fields
    # id = Column(Integer)
    # job_offer_uuid = shortuuid.uuid()
    offer_url = Column(String, primary_key=True)
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
    #  many to one
    company_name = Column(String, ForeignKey('Company.company'))
    company = relationship("Company",
                           back_populates="company_to_job_offer")
    jobsite_name = Column(String, ForeignKey('Jobsite.jobsite'))
    jobsite = relationship("Jobsite",
                           back_populates="jobsite_to_job_offer")
    #  many to many
    location = relationship("Location",
                            secondary=association_JobOffer_Location,
                            back_populates='location_to_job_offer')
    experience = relationship("Experience",
                              secondary=association_JobOffer_Experience,
                              back_populates='experience_to_job_offer')
    employment_type = relationship("Employment_type",
                                   secondary=association_JobOffer_Employment_type,
                                   back_populates='employment_type_to_job_offer')
    skill_must = relationship("Skill",
                              secondary=association_JobOffer_Skill_must,
                              back_populates='skill_must_to_job_offer')
    skill_nice = relationship("Skill",
                              secondary=association_JobOffer_Skill_nice,
                              back_populates='skill_nice_to_job_offer')


class Company(Base):
    __tablename__ = 'Company'
    #  fields
    # id = Column(Integer, primary_key=True)
    # company_uuid = Column(String, primary_key=True)
    company = Column(String, primary_key=True)
    company_size = Column(String)
    #  one to many
    company_to_job_offer = relationship("JobOffer",
                                        back_populates='company')


class Jobsite(Base):
    __tablename__ = 'Jobsite'
    #  fields
    # id = Column(Integer, primary_key=True)
    # jobsite_uuid = Column(String, primary_key=True)
    jobsite = Column(String, primary_key=True)
    #  one to many
    jobsite_to_job_offer = relationship("JobOffer",
                                        back_populates='jobsite')


class Location(Base):
    __tablename__ = 'Location'
    #  fields
    # id = Column(Integer, primary_key=True)
    # location_uuid = Column(String, primary_key=True)
    location = Column(String, primary_key=True)
    #  many to many
    location_to_job_offer = relationship("JobOffer",
                                         secondary=association_JobOffer_Location,
                                         back_populates='location')


class Experience(Base):
    __tablename__ = 'Experience'
    #  fields
    # id = Column(Integer, primary_key=True)
    # experience_uuid = Column(String, primary_key=True)
    experience = Column(String, primary_key=True)
    #  many to many
    experience_to_job_offer = relationship("JobOffer",
                                           secondary=association_JobOffer_Experience,
                                           back_populates='experience')


class Employment_type(Base):
    __tablename__ = "Employment_type"
    #  fields
    # id = Column(Integer, primary_key=True)
    # employment_type_uuid = Column(String, primary_key=True)
    employment_type = Column(String, primary_key=True)
    #  many to many
    employment_type_to_job_offer = relationship("JobOffer",
                                                secondary=association_JobOffer_Employment_type,
                                                back_populates='employment_type')


class Skill(Base):
    __tablename__ = "Skill"
    #  fields
    # id = Column(Integer, primary_key=True)
    # skill_uuid = Column(String, primary_key=True)
    skill = Column(String, primary_key=True)
    # many to many
    skill_must_to_job_offer = relationship("JobOffer",
                                           secondary=association_JobOffer_Skill_must,
                                           back_populates='skill_must')
    skill_nice_to_job_offer = relationship("JobOffer",
                                           secondary=association_JobOffer_Skill_nice,
                                           back_populates='skill_nice')


