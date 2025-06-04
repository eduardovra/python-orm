class Column:
    def __init__(self, type_, primary_key=False):
        self.type_ = type_
        self.primary_key = primary_key

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, type=None) -> object:
        return obj.__dict__.get(self.name) or 0

    def __set__(self, obj, value) -> None:
        obj.__dict__[self.name] = value

class Field:
    pass

class Integer(Field):
    pass

class String(Field):
    pass

def create_engine(connection_string: str):
    # This function would normally create a database engine
    return None


def declarative_base():
    # This function would normally return a base class for declarative models

    class Metadata:
        def create_all(self, engine):
            # This method would normally create all tables in the database
            pass

    class Base:
        metadata = Metadata()

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    return Base

def sessionmaker(bind=None):
    # This function would normally return a session class bound to the engine

    class Query:
        def __init__(self, model):
            self.model = model
            self._filter = {}

        def filter_by(self, **kwargs):
            self._filter = kwargs
            return self

        def all(self):
            # In a real implementation, this would query the database
            return [obj for obj in Session.data if all(getattr(obj, k) == v for k, v in self._filter.items())]

        def first(self):
            results = self.all()
            return results[0] if results else None

    class Session:
        data = []
        # def __init__(self):
        #     self.data = []

        def add(self, obj):
            self.data.append(obj)

        def add_all(self, objs):
            self.data.extend(objs)

        def commit(self):
            pass  # In a real implementation, this would commit the transaction

        def close(self):
            pass  # In a real implementation, this would close the session

        def query(self, model):
            return Query(model)

    return Session

# from sqlalchemy import create_engine, Column, Integer, String
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base

# Define the database connection
engine = create_engine('sqlite:///:memory:') # In memory database

# Create a base class for declarative models
Base = declarative_base()

# Define the User model
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)

# Create the database tables
Base.metadata.create_all(engine)

# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Add some users
user1 = User(name='Alice', age=30)
user2 = User(name='Bob', age=25)
session.add_all([user1, user2])
session.commit()

# Query all users
users = session.query(User).all()
for user in users:
    print(f"ID: {user.id}, Name: {user.name}, Age: {user.age}")

# Query a specific user
user = session.query(User).filter_by(name='Alice').first()
print(f"User found: ID: {user.id}, Name: {user.name}, Age: {user.age}")

# Close the session
session.close()
