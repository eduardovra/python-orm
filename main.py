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
        if obj is None:
            return self
        return obj.__dict__.get(self.name, None)

    def __set__(self, obj, value) -> None:
        obj.__dict__[self.name] = value

    def __eq__(self, other):
        return (self.name, "=", other)

    def asc(self):
        return (self.name, 'ASC')

    def desc(self):
        return (self.name, 'DESC')

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

    class Session:
        def __init__(self):
            assert bind is not None, "Session must be bound to an engine"
            self.engine = bind
            self.new = set()
            self.dirty = set()
            self.deleted = set()

        def add(self, obj):
            # Find primary key name and value
            pk_name = None
            for attr, value in obj.__class__.__dict__.items():
                if isinstance(value, Column) and value.primary_key:
                    pk_name = attr
                    break
            if pk_name is not None:
                pk_value = getattr(obj, pk_name)
                if pk_value not in (None, 0, '', False):
                    self.dirty.add(obj)  # Mark for update
                    return
            self.new.add(obj)  # Mark for insert

        def add_all(self, objs):
            for obj in objs:
                self.add(obj)

        def flush(self):
            # Insert new objects
            for obj in list(self.new):
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
                cursor = self.engine.execute(sql, values)

                # Assign primary key if it exists
                pk_name = None
                for attr, value in obj.__class__.__dict__.items():
                    if isinstance(value, Column) and value.primary_key:
                        pk_name = attr
                        break
                if pk_name is not None:
                    pk_value = cursor.lastrowid
                    setattr(obj, pk_name, pk_value)

                self.new.remove(obj)
            # Update dirty objects
            for obj in list(self.dirty):
                table = obj.__tablename__
                pk_name = None
                for attr, value in obj.__class__.__dict__.items():
                    if isinstance(value, Column) and value.primary_key:
                        pk_name = attr
                        break
                if pk_name is None:
                    continue
                pk_value = getattr(obj, pk_name)
                fields = []
                values = []
                for attr, value in obj.__dict__.items():
                    if not attr.startswith('_') and attr != pk_name:
                        fields.append(f"{attr}=?")
                        values.append(value)
                sql = f"UPDATE {table} SET {', '.join(fields)} WHERE {pk_name}=?"
                values.append(pk_value)
                self.engine.execute(sql, values)
                self.dirty.remove(obj)
            # Delete deleted objects
            for obj in list(self.deleted):
                table = obj.__tablename__
                pk_name = None
                for attr, value in obj.__class__.__dict__.items():
                    if isinstance(value, Column) and value.primary_key:
                        pk_name = attr
                        break
                if pk_name is None:
                    continue
                pk_value = getattr(obj, pk_name)
                sql = f"DELETE FROM {table} WHERE {pk_name}=?"
                self.engine.execute(sql, (pk_value,))
                self.deleted.remove(obj)

        def commit(self):
            self.flush()
            self.engine.commit()

        def update(self, obj, **kwargs):
            for k, v in kwargs.items():
                setattr(obj, k, v)
            self.dirty.add(obj)

        def delete(self, obj):
            self.deleted.add(obj)

        def query(self, model):
            return Query(model, self)

        def close(self):
            self.engine.close()

    class Query:
        def __init__(self, model, session):
            self.model = model
            self.session = session
            self._filter = {}
            self._filter_exprs = []
            self._limit = None
            self._order_by = None

        def filter_by(self, **kwargs):
            query = self

            for k, v in kwargs.items():
                column = getattr(self.model, k)
                assert isinstance(column, Column), f"{k} is not a valid column"
                query = query.filter(column == v)

            return query

        def filter(self, *exprs):
            self._filter_exprs.extend(exprs)
            return self

        def limit(self, n):
            self._limit = n
            return self

        def order_by(self, *columns):
            # Accepts columns, or columns with .asc()/.desc()
            self._order_by = columns
            return self

        def _build_where_clause(self):
            conditions = []
            params = []
            for expr in self._filter_exprs:
                col, op, val = expr
                conditions.append(f"{col} {op} ?")
                params.append(val)
            if conditions:
                return " WHERE " + " AND ".join(conditions), params
            return "", []

        def _build_order_by_clause(self):
            if self._order_by:
                order_clauses = []
                for col in self._order_by:
                    if isinstance(col, tuple):
                        order_clauses.append(f"{col[0]} {col[1]}")
                    elif isinstance(col, Column):
                        order_clauses.append(col.name)
                return " ORDER BY " + ", ".join(order_clauses)
            return ""

        def _build_limit_clause(self):
            if self._limit is not None:
                return f" LIMIT {self._limit}"
            return ""

        def _build_sql_clauses(self):
            where_clause, params = self._build_where_clause()
            order_by_clause = self._build_order_by_clause()
            limit_clause = self._build_limit_clause()
            sql = where_clause + order_by_clause + limit_clause
            return sql, params

        def first(self):
            for result in self.limit(1).all():
                return result

        def last(self):
            if results := self.all():
                return results[-1]

        def all(self):
            table = getattr(self.model, '__tablename__', self.model.__name__.lower())
            sql = f"SELECT * FROM {table}"
            clause, params = self._build_sql_clauses()
            sql += clause

            cursor = self.session.engine.execute(sql, params)
            rows = cursor.fetchall()
            results = []
            for row in rows:
                obj = self.model()
                for idx, col in enumerate(cursor.description):
                    setattr(obj, col[0], row[idx])
                results.append(obj)
            return results

        def update(self, **kwargs):
            table = getattr(self.model, '__tablename__', self.model.__name__.lower())
            set_clauses = []
            set_values = []
            for k, v in kwargs.items():
                set_clauses.append(f"{k}=?")
                set_values.append(v)
            sql = f"UPDATE {table} SET {', '.join(set_clauses)}"
            clause, params = self._build_sql_clauses()
            sql += clause
            all_params = set_values + params
            self.session.engine.execute(sql, all_params)

        def delete(self):
            table = getattr(self.model, '__tablename__', self.model.__name__.lower())
            sql = f"DELETE FROM {table}"
            clause, params = self._build_sql_clauses()
            sql += clause
            self.session.engine.execute(sql, params)

    return Session
