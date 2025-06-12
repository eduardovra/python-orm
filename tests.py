import unittest
import sys

from main import (
    create_engine, declarative_base, sessionmaker, Column,
    Integer, String, Boolean, Date, DateTime, Text,
)
import datetime


Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    is_active = Column(Boolean)
    bio = Column(Text)
    birthday = Column(Date)
    created_at = Column(DateTime)


class TestQuery(unittest.TestCase):
    def setUp(self):
        # Setup in-memory DB and model
        self.engine = create_engine('sqlite://:memory:')
        self.engine.set_trace_callback(print)  # Optional: for debugging SQL queries

        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Add initial users
        user1 = User(name='Alice', age=30)
        user2 = User(name='Bob', age=25)
        self.session.add_all([user1, user2])
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_query_all_users(self):
        users = self.session.query(User).all()
        names = sorted([u.name for u in users])
        self.assertEqual(names, ['Alice', 'Bob'])

    def test_query_specific_user(self):
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, 'Alice')
        self.assertEqual(user.age, 30)

    def test_query_using_model_attribute(self):
        user = self.session.query(User).filter(User.name == 'Alice', User.age == 30).first()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, 'Alice')

    def test_update_user_with_query(self):
        self.session.query(User).filter_by(name='Alice').update(name='Alicia')
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alicia').first()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, 'Alicia')

    def test_update_user_instance(self):
        user = self.session.query(User).filter_by(name='Alice').first()
        user.age = 31
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.age, 31)

    def test_delete_user(self):
        self.session.query(User).filter_by(name='Bob').delete()
        self.session.commit()
        users = self.session.query(User).all()
        names = [u.name for u in users]
        self.assertNotIn('Bob', names)
        self.assertIn('Alice', names)

    def test_query_first_user(self):
        user = self.session.query(User).order_by(User.name).first()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, 'Alice')

    def test_query_last_user(self):
        user = self.session.query(User).order_by(User.name).last()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, 'Bob')

    def test_order_by_name_asc(self):
        users = self.session.query(User).order_by(User.name.asc()).all()
        names = [u.name for u in users]
        self.assertEqual(names, ['Alice', 'Bob'])

    def test_order_by_name_desc(self):
        users = self.session.query(User).order_by(User.name.desc()).all()
        names = [u.name for u in users]
        self.assertEqual(names, ['Bob', 'Alice'])

    def test_group_by_age(self):
        # Add another user with the same age as Bob
        user3 = User(name='Charlie', age=25)
        self.session.add(user3)
        self.session.commit()
        # Group by age and count users per age
        sql = "SELECT age, COUNT(*) as count FROM users GROUP BY age"
        cursor = self.session.engine.execute(sql)
        results = cursor.fetchall()
        age_counts = {row[0]: row[1] for row in results}
        self.assertEqual(age_counts[25], 2)
        self.assertEqual(age_counts[30], 1)

    def test_group_by_query_api(self):
        # Add another user with the same age as Bob
        user3 = User(name='Charlie', age=25)
        self.session.add(user3)
        self.session.commit()
        # Use the ORM's group_by API
        results = self.session.query(User).group_by(User.age).all()
        ages = sorted([u.age for u in results])
        self.assertEqual(ages, [25, 30])

    def test_filter_eq(self):
        user = self.session.query(User).filter(User.name == 'Alice').first()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, 'Alice')

    def test_filter_ne(self):
        users = self.session.query(User).filter(User.name != 'Alice').all()
        names = [u.name for u in users]
        self.assertIn('Bob', names)
        self.assertNotIn('Alice', names)

    def test_filter_lt(self):
        users = self.session.query(User).filter(User.age < 30).all()
        names = [u.name for u in users]
        self.assertEqual(names, ['Bob'])

    def test_filter_le(self):
        users = self.session.query(User).filter(User.age <= 25).all()
        names = [u.name for u in users]
        self.assertEqual(names, ['Bob'])

    def test_filter_gt(self):
        users = self.session.query(User).filter(User.age > 25).all()
        names = [u.name for u in users]
        self.assertEqual(names, ['Alice'])

    def test_filter_ge(self):
        users = self.session.query(User).filter(User.age >= 30).all()
        names = [u.name for u in users]
        self.assertEqual(names, ['Alice'])

    def test_integer_column(self):
        user = self.session.query(User).filter_by(name='Alice').first()
        user.age = 42
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.age, 42)
        user.age = 27
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.age, 27)

    def test_string_column(self):
        user = self.session.query(User).filter_by(name='Alice').first()
        user.name = "Alicia"
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alicia').first()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, "Alicia")
        user.name = "Alice"
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, "Alice")

    def test_boolean_column(self):
        user = self.session.query(User).filter_by(name='Alice').first()
        user.is_active = True
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertTrue(user.is_active)
        user.is_active = False
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertFalse(user.is_active)

    def test_text_column(self):
        user = self.session.query(User).filter_by(name='Alice').first()
        user.bio = "Hello world!"
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.bio, "Hello world!")
        user.bio = "Updated bio"
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.bio, "Updated bio")

    def test_date_column(self):
        user = self.session.query(User).filter_by(name='Alice').first()
        user.birthday = datetime.date(2000, 1, 1)
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.birthday, datetime.date(2000, 1, 1))
        user.birthday = datetime.date(1999, 12, 31)
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.birthday, datetime.date(1999, 12, 31))

    def test_datetime_column(self):
        user = self.session.query(User).filter_by(name='Alice').first()
        dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        user.created_at = dt
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.created_at, dt)
        new_dt = datetime.datetime(2024, 6, 11, 8, 30, 0)
        user.created_at = new_dt
        self.session.add(user)
        self.session.commit()
        user = self.session.query(User).filter_by(name='Alice').first()
        self.assertEqual(user.created_at, new_dt)


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
