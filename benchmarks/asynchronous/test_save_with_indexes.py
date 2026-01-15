import asyncio

from mongoengine import Document, IntField, StringField
from mongoengine import async_connect, async_disconnect


class User0(Document):
    name = StringField()
    age = IntField()
    meta = {"collection": "company"}


class User1(Document):
    name = StringField()
    age = IntField()
    meta = {"indexes": [["name"]]}


class User2(Document):
    name = StringField()
    age = IntField()
    meta = {"indexes": [["name", "age"]]}


class User3(Document):
    name = StringField()
    age = IntField()
    meta = {"indexes": [["name"]], "auto_create_index_on_save": True}


class User4(Document):
    name = StringField()
    age = IntField()
    meta = {"indexes": [["name", "age"]], "auto_create_index_on_save": True}


class TestSaveWithIndexes:

    @staticmethod
    async def drop_collections():
        await User0.adrop_collection()
        await User1.adrop_collection()
        await User2.adrop_collection()
        await User3.adrop_collection()
        await User4.adrop_collection()

    @classmethod
    def setup_class(cls):
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)
        cls.loop.run_until_complete(cls.connect())
        cls.loop.run_until_complete(cls.drop_collections())
        cls.loop.run_until_complete(cls.disconnect())


    @classmethod
    def teardown_class(cls):
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)
        cls.loop.run_until_complete(cls.connect())
        cls.loop.run_until_complete(cls.drop_collections())
        cls.loop.run_until_complete(cls.disconnect())

    @staticmethod
    async def connect():
        await async_connect(db="testDB")

    @staticmethod
    async def disconnect():
        await async_disconnect()

    def async_benchmark(self, benchmark, func):
        async_loop = asyncio.new_event_loop()
        async_loop.run_until_complete(self.connect())

        def run(event_loop):
            return event_loop.run_until_complete(
                func()
            )

        benchmark(run, async_loop)
        async_loop.run_until_complete(self.disconnect())

    def test_doc_without_index(self, benchmark):
        self.async_benchmark(benchmark, lambda: User0(name="Nunu", age=9).asave())

    def test_doc_with_1_index(self, benchmark):
        self.async_benchmark(benchmark, lambda: User1(name="Nunu", age=9).asave())

    def test_doc_with_2_index(self, benchmark):
        self.async_benchmark(benchmark, lambda: User2(name="Nunu", age=9).asave())

    def test_doc_with_1_auto_created_index(self, benchmark):
        self.async_benchmark(benchmark, lambda: User3(name="Nunu", age=9).asave())

    def test_doc_with_2_auto_created_index(self, benchmark):
        self.async_benchmark(benchmark, lambda: User4(name="Nunu", age=9).asave())
