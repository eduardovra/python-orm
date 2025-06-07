import sqlite3

# from sqlalchemy import create_engine, Column, Integer, String
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base

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
    if connection_string.startswith('sqlite://'):
        return sqlite3.connect(connection_string.replace('sqlite://', ''))
    else:
        raise ValueError("Unsupported database type")


def declarative_base():
    # This function would normally return a base class for declarative models

    class Metadata:
        def create_all(self, engine):
            # Find all subclasses of Base (i.e., all models)
            for cls in Base.__subclasses__():
                table = getattr(cls, '__tablename__', cls.__name__.lower())
                columns = []
                for attr, value in cls.__dict__.items():
                    if isinstance(value, Column):
                        col_type = value.type_
                        if col_type is Integer:
                            sql_type = "INTEGER"
                        elif col_type is String:
                            sql_type = "VARCHAR"
                        else:
                            raise NotImplementedError(f"Unsupported column type: {col_type}")
                        col_def = f"{attr} {sql_type}"
                        if value.primary_key:
                            col_def += " PRIMARY KEY"
                        columns.append(col_def)
                sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})"
                engine.execute(sql)

    class Base:
        metadata = Metadata()

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        def __repr__(self):
            attrs = ', '.join(f"{k}={v}" for k, v in self.__dict__.items() if not k.startswith('_'))
            return f"{self.__class__.__name__}({attrs})"

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
            table = getattr(self.model, '__tablename__', self.model.__name__.lower())
            sql = f"SELECT * FROM {table}"
            params = []
            if self._filter:
                conditions = []
                for k, v in self._filter.items():
                    conditions.append(f"{k}=?")
                    params.append(v)
                sql += " WHERE " + " AND ".join(conditions)
            cursor = Session.engine.execute(sql, params)
            rows = cursor.fetchall()
            results = []
            for row in rows:
                # Map row to model instance
                obj = self.model()
                for idx, col in enumerate(cursor.description):
                    setattr(obj, col[0], row[idx])
                results.append(obj)
            return results

        def first(self):
            results = self.all()
            return results[0] if results else None

    class Session:
        engine = bind
        objs = []

        def add(self, obj):
            self.objs.append(obj)

        def add_all(self, objs):
            self.objs.extend(objs)

        def flush(self):
            # Insert all objects in self.objs into the database
            for obj in self.objs:
                table = obj.__tablename__
                fields = []
                values = []
                placeholders = []
                for attr, value in obj.__dict__.items():
                    if not attr.startswith('_'):
                        fields.append(attr)
                        values.append(value)
                        placeholders.append('?')
                sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                self.engine.execute(sql, values)
            self.objs.clear()

        def commit(self):
            if self.objs:
                self.flush()
            self.engine.commit()

        def close(self):
            self.engine.close()

        def query(self, model):
            return Query(model)

    return Session

# Define the database connection
engine = create_engine('sqlite://:memory:') # In memory database
engine.set_trace_callback(print)

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
    print(f"  {user}")

# Query a specific user
user = session.query(User).filter_by(name='Alice').first()
print(f"  {user}")

# Close the session
session.close()
