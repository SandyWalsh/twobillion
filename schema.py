import sqlalchemy
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref

engine = sqlalchemy.create_engine('mysql://root:password@localhost/cm_meta', 
                                  use_ansiquotes=True, echo=True)

Base = declarative_base()


class RawData(Base):
    __tablename__ = 'rawdata'
    id = Column(Integer, primary_key=True)
    raw = Column(Text)

    def __init__(self, _raw):
        self.raw = _raw

    def __repr__(self):
        return "<RawData('%s')>" % self.raw


class Trait(Base):
    __tablename__ = 'traits'

    id = Column(Integer, primary_key=True)
    key = Column(String(32), index=True)
    value = Column(String(32), index=True)

    rawdata_id = Column(Integer, ForeignKey('rawdata.id'))
    raw_data = relationship("RawData", backref=backref('raw_data', order_by=id))

    def __init__(self, key, value, data):
        self.key = key
        self.value = value
        self.raw_data = data

    def __repr__(self):
        return "<Trait('%s','%s')>" % (self.key, self.value)


def get_session():
    Session = sessionmaker(bind=engine)
    session = Session()

    return session

def reset_db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine) 
