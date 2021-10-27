from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, delete, PickleType
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import update
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import datetime, timedelta
import pickle
import os


DATABASE_FILE = "database.sqlite"

engine = create_engine('sqlite:///%s' % (DATABASE_FILE), echo=False)
conn = engine.connect()

Base = declarative_base()

class Files(Base):
    __tablename__ = 'Files'
    id = Column(Integer, primary_key=True)
    user = Column(Integer)
    upload_timestamp = Column(String)
    file_name = Column(String)
    time_views = Column(PickleType)
    start_date = Column(String)
    end_date = Column(String)
    ports_dyn3_x = Column(PickleType)

    def to_dictionary(self):
        return {"file_id": self.id, "user": self.user, "upload_timestamp": self.upload_timestamp,
                "file_name": self.file_name, "time_views": self.time_views, "start_date": self.start_date,
                "end_date": self.end_date, "ports_dyn3_x": self.ports_dyn3_x  }


Base.metadata.create_all(engine)  # Create tables for the data models

Session = sessionmaker(bind=engine)
session = scoped_session(Session)
Session = Session()



def insert_file(user, file_name, time_views, start_date, end_date, ports):
    # check if file already exist, remove if exist
    file_id = get_file_id(file_name, user)
    if file_id != 0:
        object_to_remove = session.query(Files).filter(
            Files.id == file_id and Files.user == user).one()
        session.delete(object_to_remove)
        session.commit()

    now = datetime.now()
    current_time = now.strftime('%Y-%m-%d %H:%M:%S')
    object_to_add = Files(user=user, upload_timestamp=current_time,
                          file_name=file_name, time_views=time_views,
                          start_date=start_date, end_date=end_date, 
                          ports_dyn3_x = ports)
    
    Session.add(object_to_add)
    Session.commit()
    Session.close()


def drop_files_table():
    conn = engine.connect()
    conn.execute("DROP TABLE Files;")
    print('DROP --- TABLE Files')
    session.commit()
    Base.metadata.create_all(engine)
    session.commit()


def get_view_activity(file_id, time):   
    ret = {}
    result = session.query(Files).filter(Files.id == file_id).all()
    data = pickle.loads(result[0].to_dictionary()['time_views'])
    ret['times'] = list(data[time].index.strftime('%Y-%m-%d %H:%M:%S'))    
    ret['values'] = data[time].to_list()

    return ret

def get_dyn3_x_ports(file_id):
    result = session.query(Files).filter(Files.id == file_id)
    data = pickle.loads(result[0].to_dictionary()['ports_dyn3_x'])
    
    return data
    
def get_file_start_end_date(file_id):    
    try:
        result = session.query(Files).filter(
            Files.id == file_id).first()

        return str(result.to_dictionary()['start_date']), str(result.to_dictionary()['end_date'])
    except:
        return None


def get_file_id(file_name, user):
    try:
        result = session.query(Files).filter(
            Files.file_name == file_name, Files.user == user).first()
        session.close()       
        return result.to_dictionary()['file_id']
    except:
        return 0

def get_user_file(file_id):
    try:
        result = session.query(Files).filter(
            Files.id == file_id).first()
        session.close()
        return result.to_dictionary()['user']
    except:
        return 0

def get_day_start_end_date(file_name, user):
    file_id = get_file_id(file_name, user)
    s_date, e_date = get_file_start_end_date(file_id)
    start_date = datetime.strptime(s_date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(e_date, '%Y-%m-%d %H:%M:%S')
    #start_date, end_date =  datetime(2018,3,2,0,0,0),datetime(2018,3,2,23,59,0)
    delta_days = end_date - start_date
    start_day = start_date.date()
    lst_days = [start_day]
    count = int(delta_days.days)
    aux_day = start_day
    while count != 0:
        start_day = start_day + timedelta(days=1)
        lst_days.append(start_day)
        count -= 1

    return [lst_days, start_date, end_date]

def get_day_start_end_date_by_id(file_id):
    result = session.query(Files).filter(Files.id == file_id).first()
    file_name = result.to_dictionary()['file_name']
    user = result.to_dictionary()['user']
    return get_day_start_end_date(file_name, user)
    
def get_file_name(file_id):
    try:
        result = session.query(Files).filter(
            Files.id == file_id).first()
        session.close()
        return result.to_dictionary()['file_name']
    except:
        return ''


def delete_file(file_id):

    try:
        file_info = session.query(Files).filter(Files.id == file_id).first().to_dictionary()
        session.query(Files).filter(Files.id == file_id).delete()
        session.commit()
        session.close()        
        os.remove('Data_files/user_'+str(file_info['user'])+'/'+file_info['file_name'])
        
        return True
    except Exception as e:
        
        print('ERROR ' + str(e))
        return False
