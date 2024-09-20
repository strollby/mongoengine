import unittest

import pytest

from mongoengine import (
    Document,
    IntField,
    ListField,
    StringField,
    connect,
)


class Doc(Document):
    id = IntField(primary_key=True)
    value = IntField()


class TestFindAndModify(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        connect(db="mongoenginetest")
        await Doc.drop_collection()

    async def _assert_db_equal(self, docs):
        assert await Doc._collection.find().sort("id").to_list() == docs

    async def test_modify(self):
        await Doc(id=0, value=0).save()
        doc = await Doc(id=1, value=1).save()

        old_doc = await Doc.objects(id=1).modify(set__value=-1)
        assert old_doc.to_json() == doc.to_json()
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    async def test_modify_full_response_raise_value_error_for_recent_mongo(self):
        await Doc(id=0, value=0).save()
        await Doc(id=1, value=1).save()

        with pytest.raises(ValueError):
            await Doc.objects(id=1).modify(set__value=-1, full_response=True)

    async def test_modify_with_new(self):
        await Doc(id=0, value=0).save()
        doc = await Doc(id=1, value=1).save()

        new_doc = await Doc.objects(id=1).modify(set__value=-1, new=True)
        doc.value = -1
        assert new_doc.to_json() == doc.to_json()
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    async def test_modify_not_existing(self):
        await Doc(id=0, value=0).save()
        assert await Doc.objects(id=1).modify(set__value=-1) is None
        await self._assert_db_equal([{"_id": 0, "value": 0}])

    async def test_modify_with_upsert(self):
        await Doc(id=0, value=0).save()
        old_doc = await Doc.objects(id=1).modify(set__value=1, upsert=True)
        assert old_doc is None
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": 1}])

    async def test_modify_with_upsert_existing(self):
        await Doc(id=0, value=0).save()
        doc = await Doc(id=1, value=1).save()

        old_doc = await Doc.objects(id=1).modify(set__value=-1, upsert=True)
        assert old_doc.to_json() == doc.to_json()
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    async def test_modify_with_upsert_with_new(self):
        await Doc(id=0, value=0).save()
        new_doc = await Doc.objects(id=1).modify(upsert=True, new=True, set__value=1)
        assert new_doc.to_mongo() == {"_id": 1, "value": 1}
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": 1}])

    async def test_modify_with_remove(self):
        await Doc(id=0, value=0).save()
        doc = await Doc(id=1, value=1).save()

        old_doc = await Doc.objects(id=1).modify(remove=True)
        assert old_doc.to_json() == doc.to_json()
        await self._assert_db_equal([{"_id": 0, "value": 0}])

    async def test_find_and_modify_with_remove_not_existing(self):
        await Doc(id=0, value=0).save()
        assert await Doc.objects(id=1).modify(remove=True) is None
        await self._assert_db_equal([{"_id": 0, "value": 0}])

    async def test_modify_with_order_by(self):
        await Doc(id=0, value=3).save()
        await Doc(id=1, value=2).save()
        await Doc(id=2, value=1).save()
        doc = await Doc(id=3, value=0).save()

        old_doc = await Doc.objects().order_by("-id").modify(set__value=-1)
        assert old_doc.to_json() == doc.to_json()
        await self._assert_db_equal(
            [
                {"_id": 0, "value": 3},
                {"_id": 1, "value": 2},
                {"_id": 2, "value": 1},
                {"_id": 3, "value": -1},
            ]
        )

    async def test_modify_with_fields(self):
        await Doc(id=0, value=0).save()
        await Doc(id=1, value=1).save()

        old_doc = await Doc.objects(id=1).only("id").modify(set__value=-1)
        assert old_doc.to_mongo() == {"_id": 1}
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    async def test_modify_with_push(self):
        class BlogPost(Document):
            tags = ListField(StringField())

        await BlogPost.drop_collection()

        blog = await BlogPost.objects.create()

        # Push a new tag via modify with new=False (default).
        await BlogPost(id=blog.id).modify(push__tags="code")
        assert blog.tags == []
        await blog.reload()
        assert blog.tags == ["code"]

        # Push a new tag via modify with new=True.
        blog = await BlogPost.objects(id=blog.id).modify(push__tags="java", new=True)
        assert blog.tags == ["code", "java"]

        # Push a new tag with a positional argument.
        blog = await BlogPost.objects(id=blog.id).modify(
            push__tags__0="python", new=True
        )
        assert blog.tags == ["python", "code", "java"]

        # Push multiple new tags with a positional argument.
        blog = await BlogPost.objects(id=blog.id).modify(
            push__tags__1=["go", "rust"], new=True
        )
        assert blog.tags == ["python", "go", "rust", "code", "java"]


if __name__ == "__main__":
    unittest.main()
