import asyncio

from pytest_benchmark.fixture import BenchmarkFixture

from mongoengine import Document, StringField, IntField, ListField, BooleanField, EmailField, EmbeddedDocument, \
    EmbeddedDocumentField
from mongoengine import async_connect, async_disconnect


class Book(Document):
    meta = {"collection": "book"}
    name = StringField()
    pages = IntField()
    tags = ListField(StringField())
    is_published = BooleanField()
    author_email = EmailField()


class Contact(EmbeddedDocument):
    name = StringField()
    title = StringField()
    address = StringField()


class Company(Document):
    meta = {"collection": "company"}
    name = StringField()
    contacts = ListField(EmbeddedDocumentField(Contact))


class TestBasicDocOps:

    @staticmethod
    async def drop_collections():
        await Book.adrop_collection()

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

    @staticmethod
    def book_doc():
        return Book(
            name="Always be closing",
            pages=100,
            tags=["-help", "sales"],
            is_published=True,
            author_email="alec@example.com",
        )

    def async_benchmark(self, benchmark, func):
        async_loop = asyncio.new_event_loop()
        async_loop.run_until_complete(self.connect())

        def run(event_loop):
            return event_loop.run_until_complete(
                func()
            )

        benchmark(run, async_loop)
        async_loop.run_until_complete(self.disconnect())

    async def doc_save_and_delete(self):
        doc = await self.book_doc().asave()
        await doc.adelete()

    def test_doc_to_mongo(self, benchmark):
        benchmark(lambda: self.book_doc().to_mongo())

    def test_doc_create(self, benchmark: BenchmarkFixture):
        benchmark(lambda: self.book_doc())

    def test_doc_save_and_delete(self, benchmark):
        self.async_benchmark(benchmark, lambda: self.doc_save_and_delete())

    def test_doc_validate(self, benchmark):
        benchmark(lambda: self.book_doc())


class TestBasicLargeDocOps:
    @staticmethod
    async def drop_collections():
        await Company.adrop_collection()

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

    @staticmethod
    def company_doc():
        return Company(
            name="MongoDB, Inc.",
            contacts=[
                Contact(name="Contact %d" % x, title="CEO", address="Address %d" % x)
                for x in range(1000)
            ],
        )

    def async_benchmark(self, benchmark, func):
        async_loop = asyncio.new_event_loop()
        async_loop.run_until_complete(self.connect())

        def run(event_loop):
            return event_loop.run_until_complete(
                func()
            )

        benchmark(run, async_loop)
        async_loop.run_until_complete(self.disconnect())

    async def big_doc_save_and_delete(self):
        doc = await self.company_doc().asave()
        await doc.adelete()

    def test_big_doc_to_mongo(self, benchmark):
        benchmark(lambda: self.company_doc().to_mongo())

    def test_big_doc_create(self, benchmark: BenchmarkFixture):
        benchmark(lambda: self.company_doc())

    def test_big_doc_save_and_delete(self, benchmark):
        self.async_benchmark(benchmark, lambda: self.big_doc_save_and_delete())

    def test_big_doc_validate(self, benchmark):
        benchmark(lambda: self.company_doc())
