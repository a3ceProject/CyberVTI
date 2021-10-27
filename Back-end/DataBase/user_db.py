from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, delete, PickleType
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker


DATABASE_Users = "database.sqlite"

engine = create_engine('sqlite:///%s' % (DATABASE_Users), echo=False)
conn = engine.connect()

Base = declarative_base()

class Users(Base):
    __tablename__ = 'Users'
    id = Column(Integer, primary_key=True)
    username = Column(String,unique=True, nullable=False)


    def to_dictionary(self):
        return {"user_id": self.id, "username": self.username}


Base.metadata.create_all(engine)  # Create tables for the data models

Session = sessionmaker(bind=engine)
session = scoped_session(Session)
Session = Session()

def insert_user(username):   
    try:
       # if not check_user:            
        object_to_add = Users(username=username)
        Session.add(object_to_add)
        Session.commit()
        Session.close()        
        return True       
        

    except Exception as e:
        Session.close() 
        print('ERROR: ' + str(e))

def delete_user(username):
    try:
        user_info = session.query(Users).filter(Users.username == username).first().to_dictionary()
        session.query(Users).filter(Users.id == user_info['user_id']).delete()
        session.commit()
        session.close()        
       
        return True
    except:
        
        print('ERROR - removing User')
        return False

def get_user_id_by_username(username):
    try:
        user_info = session.query(Users).filter(Users.username == username).first().to_dictionary()
        session.close()        
        
        return user_info['user_id']
    except:        
        print('ERROR - NO User')
        return False

def delete_user(user_id):
    try:            
        session.query(Users).filter(Users.id == user_id).delete()
        session.commit()
        session.close()      
        
        return True
    except:
        
        print('ERROR - removing User')
        return False