from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.dialects import postgresql
from sqlalchemy import update, inspect

from typing import List


class DatabaseManager:

    def __init__(self, testing: bool=False):

        self.engine = create_engine("postgresql+psycopg2://localhost/sqla_testing", echo=True)

        # What are the differences between using Session(engine) and sessionmaker(engine)?
        # Sessionmaker allows you to create new sessions without passing the engine each time.
        self.Session = sessionmaker(bind=self.engine)


    def get_session(self):
        return self.Session()

    def save(self, obj):
        with self.get_session() as sess:
            if obj not in sess:
                sess.add(obj)
            sess.commit()

    def delete(self, obj):
        with self.get_session() as sess:
            sess.delete(obj)
            sess.commit()

    # Here is the issue with update()
    # 1. You create a new session when calling save, update, delete
    # 2. It seems to be ok for save and delete for some reason
    # 3. It's not ok for an update operation. When checking for the object in session, it is False, and when you try
    #    to add it to the session to commit SQLA raises an error to let you know it is already present in a different
    #    session.
    #    You can merge it into the new session, but don't feel like this is the appropriate solution for a normal
    #    update method.


    # This works. This can be used to mixin to instance logic and not have to do so much session management.
    def update(self, obj):
        # If the obj is in a session already, find it and use it
        existing_session = inspect(obj).session
        if existing_session is not None:
            existing_session.commit()
        # Otherwise get a session, add and commit
        else:
            with self.get_session() as sess:
                sess.add(obj)
                sess.commit()


    # NOTE: This works also
    # def update(self, obj):
    #     with self.get_session() as sess:
    #         if not obj in sess:
    #             # Copy the state of a given instance into a corresponding instance within this Session.
    #             obj = sess.merge(obj)
    #             # Session.merge(obj) adds obj to this session.
    #         sess.commit()



db = DatabaseManager()


class CRUDMixin:

    def save(self):
        db.save(self)

    def update(self):
        db.update(self)

    def delete(self):
        db.delete(self)


class Base(DeclarativeBase):
    pass

class TestClass(Base, CRUDMixin):

    __tablename__ = "test_table"

    profile_id:     Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    profile:        Mapped[str] = mapped_column(nullable=False)
    num_evals:      Mapped[int] = mapped_column(default=0)
    cost:           Mapped[List] = mapped_column(postgresql.ARRAY(postgresql.REAL), default=list())
    kpis:           Mapped[List] = mapped_column(postgresql.ARRAY(postgresql.TEXT), default=list())


if __name__ == "__main__":

    engine = db.engine
    Base.metadata.drop_all(engine) # Drop all tables
    Base.metadata.create_all(engine)

    tc = TestClass(profile_id=1, profile="some magic json profile")

    # Note: Default values num_evals=0, cost=[], kpis=[] are not set until an insert or update is issued,
    # i.e. your session is flushed to the DB. The only way to set something without doing so is to create an __init__
    # and set it yourself. Note that if you pass in values in the constructor they are set (eg cost=[]).
    tc.save()

    # NOTE:
    #     db.save() uses a context manager and the session() is closed automatically.
    #     tc is now in a detached state. If you try to make changes and save, it will raise an error.
    #     There is no tc at this point
    #     You have to either:
    #     - Create a session, bring tc into scope in the session
    #     - Make changes to tc, merge it into a new session (haven't tested this, not sure if it works), don't want
    #       to do it this way anyway.

    # Note: The typical way to retrieve objects seems to be
    # obj = session.get(TestClass, {"profile_id": 1, "generation_num": 2})
    # or
    # tc = db.session.get(TestClass, 1)
    # or
    # stmt = select(TestClass).where(TestClass.profile_id == 1)
    # tc = db.get_session().scalar(stmt)

    # Get tc back
    tc = db.get_session().get(TestClass, 1)

    # Make some changes
    tc.cost.append(1234.12)
    # SQLA inspects references for changes. This is fine for primitive types, but is an issue for reference types like
    # lists. You have to tell SQLA that the list changed and should be updated by calling flag_modified(). If you don't
    # do this then the field will not be updated in the DB. Credit to
    # https://medium.com/analytics-vidhya/updating-non-primitive-columns-types-using-sqlalchemy-and-postgres-12f8206ba457
    flag_modified(tc, 'cost')
    tc.kpis.append("KPI Model 1")
    flag_modified(tc, 'kpis')
    tc.num_evals = 1

    tc.update()

    # The following works because session is maintained until commit()
    session = db.get_session()
    tc = session.get(TestClass, 1)
    # Can modify lists this way
    cost = list(tc.cost)
    cost.append(3456.78)
    tc.cost = cost
    # Or use flag_modified
    tc.kpis.append("KPI Model 2")
    flag_modified(tc, 'kpis')
    # num_evals is a primitive type, no issue.
    tc.num_evals += 1

    session.commit()
    session.close()

    # This also works.
    session = db.get_session()
    tc = session.get(TestClass, 1)
    tc.cost.append(9876.4576)
    tc.kpis.append("KPI Model 3")
    session.execute(update(TestClass).where(TestClass.profile_id == 1).values(
        {"cost": tc.cost,
         "kpis": tc.kpis,
         "num_evals": 3}
    ))
    session.commit()
    session.close()

