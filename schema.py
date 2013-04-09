import sqlalchemy
from sqlalchemy import Column, Integer, String, Text, Numeric, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref

engine = sqlalchemy.create_engine('mysql://root:password@localhost/cm_meta', 
                                  use_ansiquotes=True, echo=True)
Base = declarative_base()


TEXT_TYPE = 1
INT_TYPE = 2
FLOAT_TYPE = 3


class UniqueName(Base):
    __tablename__ = 'uniquename'
    __table_args__ = {'mysql_engine':'InnoDB'}
    id = Column(Integer, primary_key=True)
    key = Column(String(32), index=True)

    def __init__(self, key):
        self.key = key

    def __repr__(self):
        return "<UniqueName('%s>" % self.key


class RawData(Base):
    __tablename__ = 'rawdata'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = Column(Integer, primary_key=True)
    when = Column(DateTime, index=True)

    uniquename_id = Column(Integer, ForeignKey('uniquename.id'))
    unique_name = relationship("UniqueName", backref=backref('unique_name', order_by=id))

    def __init__(self, event_id, when):
        self.eventname_id = event_id
        self.when = when

    def __repr__(self):
        return "<RawData('Event: %s, When: %s')>" % (self.unique_name, self.when)


class Trait(Base):
    __tablename__ = 'traits'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = Column(Integer, primary_key=True)

    key_id = Column(Integer, ForeignKey('uniquename.id'))
    key_name = relationship("UniqueName", backref=backref('key_name', order_by=id))

    t_type = Column(Integer, index=True)
    t_string = Column(String(32), nullable=True, default=None, index=True)
    t_float = Column(Numeric, nullable=True, default=None, index=True)
    t_int = Column(Integer, nullable=True, default=None, index=True)

    rawdata_id = Column(Integer, ForeignKey('rawdata.id'))
    raw_data = relationship("RawData", backref=backref('raw_data', order_by=id))

    def __init__(self, key_id, data, t_type, t_string=None, t_float=None, t_int=None):
        self.key_id = key_id
        self.t_type = t_type
        self.raw_data = data
        self.t_string = t_string
        self.t_float = t_float
        self.t_int = t_int

    def __repr__(self):
        return "<Trait(%s) %d=%s/%s/%s on %s>" % (self.key_name, self.t_type, self.t_string,
                        self.t_float, self.t_int, self.raw_data)


def get_session():
    Session = sessionmaker(bind=engine)
    session = Session()

    return session

def reset_db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine) 
