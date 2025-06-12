import sqlite3
import datetime

class Field:
    def to_python(self, value):
        return value

    def to_db(self, value):
        return value

class Integer(Field):
    pass

class String(Field):
    pass

class Text(Field):
    pass

class Boolean(Field):
    def to_python(self, value):
        if isinstance(value, int):
            return bool(value)
        return value

    def to_db(self, value):
        if isinstance(value, bool):
            return int(value)
        return value

class Date(Field):
    def to_python(self, value):
        if isinstance(value, str):
            return datetime.date.fromisoformat(value)
        return value

    def to_db(self, value):
        if isinstance(value, datetime.date):
            return value.isoformat()
        return value

class DateTime(Field):
    def to_python(self, value):
        if isinstance(value, str):
            return datetime.datetime.fromisoformat(value)
        return value

    def to_db(self, value):
        if isinstance(value, datetime.datetime):
            return value.isoformat(sep=' ')
        return value

class Column:
    def __init__(self, type_, primary_key=False):
        self.type_ = type_
        self.primary_key = primary_key

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, type=None) -> object:
        if obj is None:
            return self
        value = obj.__dict__.get(self.name, None)
        # Use Field's to_python
        if isinstance(self.type_, Field):
            return self.type_.to_python(value)
        elif isinstance(self.type_, type) and issubclass(self.type_, Field):
            return self.type_().to_python(value)
        return value

    def __set__(self, obj, value) -> None:
        # Use Field's to_db
        if isinstance(self.type_, Field):
            value = self.type_.to_db(value)
        elif isinstance(self.type_, type) and issubclass(self.type_, Field):
            value = self.type_().to_db(value)
        obj.__dict__[self.name] = value

    def __eq__(self, other):
        return (self.name, "=", other)

    def __ne__(self, other):
        return (self.name, "<>", other)

    def __lt__(self, other):
        return (self.name, "<", other)

    def __le__(self, other):
        return (self.name, "<=", other)

    def __gt__(self, other):
        return (self.name, ">", other)

    def __ge__(self, other):
        return (self.name, ">=", other)

    def asc(self):
        return (self.name, 'ASC')

    def desc(self):
        return (self.name, 'DESC')

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
                        elif col_type is Text:
                            sql_type = "TEXT"
                        elif col_type is Boolean:
                            sql_type = "BOOLEAN"
                        elif col_type is Date:
                            sql_type = "DATE"
                        elif col_type is DateTime:
                            sql_type = "DATETIME"
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
                        # Convert types for Date, DateTime, Boolean
                        col = getattr(obj.__class__, attr)
                        if isinstance(col, Column):
                            if isinstance(col.type_, Field):
                                value = col.type_.to_db(value)
                            elif isinstance(col.type_, type) and issubclass(col.type_, Field):
                                value = col.type_().to_db(value)
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
                        # Convert types for Date, DateTime, Boolean
                        col = getattr(obj.__class__, attr, None)
                        if isinstance(col, Column):
                            if isinstance(col.type_, Field):
                                value = col.type_.to_db(value)
                            elif isinstance(col.type_, type) and issubclass(col.type_, Field):
                                value = col.type_().to_db(value)
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
            self._filter_exprs = []
            self._limit = None
            self._order_by = None
            self._group_by = None
            self._joins = []

        def join(self, other_model, on_expr):
            """
            other_model: the class of the table to join
            on_expr: a tuple (left_col, op, right_col), e.g. (User.id, '=', Post.user_id)
            """
            self._joins.append((other_model, on_expr))
            return self

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

        def group_by(self, *columns):
            self._group_by = columns
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

        def _build_group_by_clause(self):
            if self._group_by:
                group_clauses = []
                for col in self._group_by:
                    assert isinstance(col, Column)
                    group_clauses.append(col.name)
                return " GROUP BY " + ", ".join(group_clauses)
            return ""

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
            group_by_clause = self._build_group_by_clause()
            order_by_clause = self._build_order_by_clause()
            limit_clause = self._build_limit_clause()
            sql = where_clause + group_by_clause + order_by_clause + limit_clause
            return sql, params

        def _build_from_clause(self):
            table = getattr(self.model, '__tablename__', self.model.__name__.lower())
            sql = f"FROM {table}"
            for other_model, on_expr in self._joins:
                other_table = getattr(other_model, '__tablename__', other_model.__name__.lower())
                left, op, right = on_expr
                left_table = table if hasattr(self.model, left) else other_table
                right_table = other_table if left_table == table else table
                sql += f" JOIN {other_table} ON {left_table}.{left} {op} {right_table}.{right.name}"

            return sql

        def first(self):
            for result in self.limit(1).all():
                return result

        def last(self):
            if results := self.all():
                return results[-1]

        def all(self):
            from_clause = self._build_from_clause()
            sql = f"SELECT * {from_clause}"
            clause, params = self._build_sql_clauses()
            sql += clause
            cursor = self.session.engine.execute(sql, params)
            rows = cursor.fetchall()
            results = []
            for row in rows:
                obj = self.model()
                for idx, col in enumerate(cursor.description):
                    col_name = col[0]
                    value = row[idx]
                    column = getattr(self.model, col_name, None)
                    if isinstance(column, Column):
                        if isinstance(column.type_, Field):
                            value = column.type_.to_python(value)
                        elif isinstance(column.type_, type) and issubclass(column.type_, Field):
                            value = column.type_().to_python(value)
                    obj.__dict__[col_name] = value
                results.append(obj)

            return results

        def update(self, **kwargs):
            table = getattr(self.model, '__tablename__', self.model.__name__.lower())
            set_clauses = []
            set_values = []
            for k, v in kwargs.items():
                column = getattr(self.model, k, None)
                if isinstance(column, Column):
                    if isinstance(column.type_, Field):
                        v = column.type_.to_db(v)
                    elif isinstance(column.type_, type) and issubclass(column.type_, Field):
                        v = column.type_().to_db(v)
                set_clauses.append(f"{k}=?")
                set_values.append(v)
            from_clause = self._build_from_clause()
            sql = f"UPDATE {table} SET {', '.join(set_clauses)}"
            # Note: SQLite does not support JOIN in UPDATE, but this is for generality
            if self._joins:
                sql += f" {from_clause[len(f'FROM {table}'):]}"  # add JOIN ... part only
            clause, params = self._build_sql_clauses()
            sql += clause
            all_params = set_values + params
            self.session.engine.execute(sql, all_params)

        def delete(self):
            table = getattr(self.model, '__tablename__', self.model.__name__.lower())
            from_clause = self._build_from_clause()
            sql = f"DELETE FROM {table}"
            # Note: SQLite does not support JOIN in DELETE, but this is for generality
            if self._joins:
                sql += f" {from_clause[len(f'FROM {table}'):]}"  # add JOIN ... part only
            clause, params = self._build_sql_clauses()
            sql += clause
            self.session.engine.execute(sql, params)

    return Session
