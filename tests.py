import unittest
from main import create_engine, declarative_base, sessionmaker, Column, Integer, String


Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)


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


if __name__ == '__main__':
    unittest.main()
