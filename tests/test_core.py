import pytest
from mongoagg.core import AggBuilder
from mongoagg.exceptions import InvalidLookupError, PipelineSerializationError, InvalidStageError

def test_basic_pipeline_construction():
    builder = AggBuilder()
    pipeline = builder.match({"status": "active"}).project({"name": 1}).build()
    
    assert len(pipeline) == 2
    assert pipeline[0] == {"$match": {"status": "active"}}
    assert pipeline[1] == {"$project": {"name": 1}}

def test_match_stage():
    builder = AggBuilder()
    pipeline = builder.match({"age": {"$gt": 18}}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$match": {"age": {"$gt": 18}}}

def test_project_stage():
    builder = AggBuilder()
    pipeline = builder.project({"name": 1, "age": 1, "_id": 0}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$project": {"name": 1, "age": 1, "_id": 0}}

def test_sort_stage():
    builder = AggBuilder()
    pipeline = builder.sort({"age": 1, "name": -1}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$sort": {"age": 1, "name": -1}}

def test_limit_and_skip():
    builder = AggBuilder()
    pipeline = builder.skip(5).limit(10).build()
    
    assert len(pipeline) == 2
    assert pipeline[0] == {"$skip": 5}
    assert pipeline[1] == {"$limit": 10}

def test_unwind_stage():
    builder = AggBuilder()
    pipeline = builder.unwind("$tags").build()
    
    print(pipeline)
    assert len(pipeline) == 1
    assert pipeline[0] == {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}}

def test_group_stage():
    builder = AggBuilder()
    pipeline = builder.group("$department", total={"$sum": "$salary"}).build()
    
    print(pipeline)
    assert len(pipeline) == 1
    assert pipeline[0] == {"$group": {"_id": "$department", "total": {"$sum": "$salary"}}}

def test_lookup_stage():
    builder = AggBuilder()
    pipeline = builder.lookup(
        from_="orders",
        local_field="userId",
        foreign_field="userId",
        as_="orders"
    ).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {
        "$lookup": {
            "from": "orders",
            "localField": "userId",
            "foreignField": "userId",
            "as": "orders"
        }
    }

def test_lookup_with_pipeline():
    builder = AggBuilder()
    sub_pipeline = AggBuilder().match({"status": "completed"}).build()
    pipeline = builder.lookup(
        from_="orders",
        as_="completed_orders",
        let_={"userId": "$_id"},
        pipeline=sub_pipeline
    ).build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$lookup"]["from"] == "orders"
    assert pipeline[0]["$lookup"]["as"] == "completed_orders"
    assert pipeline[0]["$lookup"]["let"] == {"userId": "$_id"}
    assert len(pipeline[0]["$lookup"]["pipeline"]) == 1

def test_invalid_lookup_error():
    builder = AggBuilder()
    with pytest.raises(InvalidLookupError):
        builder.lookup(
            from_="orders",
            as_="orders"
        ).build()

def test_invalid_stage_error():
    builder = AggBuilder()
    with pytest.raises(InvalidStageError):
        builder.add({"invalid": "stage"}).build()

def test_pipeline_serialization():
    builder = AggBuilder()
    pipeline = builder.match({"status": "active"}).project({"name": 1}).build()
    json_str = builder.to_json()
    
    assert isinstance(json_str, str)
    assert "status" in json_str
    assert "name" in json_str

def test_complex_pipeline():
    builder = AggBuilder()
    pipeline = (
        builder
        .match({"status": "active"})
        .lookup(
            from_="orders",
            local_field="userId",
            foreign_field="userId",
            as_="orders"
        )
        .unwind("$orders")
        .group("$department", 
            total_orders={"$sum": 1},
            avg_amount={"$avg": "$orders.amount"}
        )
        .sort({"total_orders": -1})
        .limit(10)
    ).build()
    
    assert len(pipeline) == 6
    assert pipeline[0]["$match"]["status"] == "active"
    assert pipeline[1]["$lookup"]["from"] == "orders"
    assert pipeline[2]["$unwind"]["path"] == "$orders"
    assert pipeline[3]["$group"]["_id"] == "$department"
    assert pipeline[4]["$sort"]["total_orders"] == -1
    assert pipeline[5]["$limit"] == 10

def test_safe_project():
    builder = AggBuilder()
    pipeline = builder.safe_project(["name", "age", "email"]).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$project": {"name": 1, "age": 1, "email": 1}}

def test_add_fields():
    builder = AggBuilder()
    pipeline = builder.add_fields({
        "total": {"$add": ["$price", "$tax"]},
        "discounted": {"$multiply": ["$price", 0.9]}
    }).build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$addFields"]["total"]["$add"] == ["$price", "$tax"]
    assert pipeline[0]["$addFields"]["discounted"]["$multiply"] == ["$price", 0.9]

def test_replace_root():
    builder = AggBuilder()
    pipeline = builder.replace_root({"$arrayElemAt": ["$items", 0]}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$replaceRoot"]["newRoot"]["$arrayElemAt"] == ["$items", 0]

def test_facet():
    builder = AggBuilder()
    pipeline = builder.facet(
        categories=[{"$group": {"_id": "$category"}}],
        total=[{"$count": "total"}]
    ).build()
    
    assert len(pipeline) == 1
    assert "categories" in pipeline[0]["$facet"]
    assert "total" in pipeline[0]["$facet"]

def test_redact():
    builder = AggBuilder()
    pipeline = builder.redact({
        "$cond": {
            "if": {"$eq": ["$level", 5]},
            "then": "$$DESCEND",
            "else": "$$PRUNE"
        }
    }).build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$redact"]["$cond"]["if"]["$eq"] == ["$level", 5]

def test_comment():
    builder = AggBuilder()
    pipeline = builder.comment("Find active users").build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$comment"] == "Find active users"
